#!/usr/bin/env python3
"""
DAG-based Test Execution Script for TestZeus SDK.

This script executes TestZeus tests according to a DAG (Directed Acyclic Graph)
structure, handling dependencies and parallel execution with fail-fast logic.

Usage:
    python dag_test_runner.py --config dag_config.json --output results.json

    # Or with explicit credentials
    python dag_test_runner.py --config dag_config.json --email user@example.com --password secret
"""

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from testzeus_sdk import TestZeusClient


# =============================================================================
# Data Classes
# =============================================================================

class NodeStatus(Enum):
    """Status of each DAG node."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CRASHED = "crashed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"  # Node not executed because a dependency failed


@dataclass
class NodeResult:
    """Result of a single DAG node execution."""
    step_name: str
    test_id: str
    status: NodeStatus = NodeStatus.PENDING
    group_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    raw_status: Optional[str] = None


@dataclass
class DAGExecutionState:
    """Tracks the state of the entire DAG execution."""
    test_map: Dict[str, str]
    dag: Dict[str, List[str]]
    levels: List[Set[str]] = field(default_factory=list)
    node_results: Dict[str, NodeResult] = field(default_factory=dict)
    completed_nodes: Set[str] = field(default_factory=set)
    failed_nodes: Set[str] = field(default_factory=set)
    crashed_nodes: Set[str] = field(default_factory=set)
    cancelled_nodes: Set[str] = field(default_factory=set)
    skipped_nodes: Set[str] = field(default_factory=set)  # Nodes not run due to failed dependencies


# =============================================================================
# DAG Processing Functions
# =============================================================================

def validate_dag(test_map: Dict[str, str], dag: Dict[str, List[str]]) -> None:
    """
    Validate the DAG configuration.

    Args:
        test_map: Mapping of step names to test IDs
        dag: DAG structure (step -> dependencies)

    Raises:
        ValueError: If validation fails
    """
    # Check all DAG nodes have entries in test_map
    for step in dag.keys():
        if step not in test_map:
            raise ValueError(f"Step '{step}' in DAG not found in test_map")

    # Check all dependencies exist
    for step, deps in dag.items():
        for dep in deps:
            if dep not in dag:
                raise ValueError(f"Dependency '{dep}' of step '{step}' not found in DAG")

    # Check for self-dependencies
    for step, deps in dag.items():
        if step in deps:
            raise ValueError(f"Step '{step}' cannot depend on itself")


def compute_execution_levels(dag: Dict[str, List[str]]) -> List[Set[str]]:
    """
    Compute topologically sorted levels for parallel execution using Kahn's algorithm.

    Each level contains nodes that can execute in parallel.

    Args:
        dag: Dictionary mapping step_name -> list of dependency step_names

    Returns:
        List of sets, where each set is a level of nodes that can run in parallel

    Raises:
        ValueError: If cycle detected in DAG
    """
    in_degree: Dict[str, int] = defaultdict(int)
    dependents: Dict[str, List[str]] = defaultdict(list)
    all_nodes: Set[str] = set(dag.keys())

    # Initialize in-degree for all nodes
    for node in all_nodes:
        in_degree[node] = len(dag.get(node, []))

    # Build dependents map (reverse edges)
    for node, dependencies in dag.items():
        for dep in dependencies:
            dependents[dep].append(node)

    # Find all nodes with no dependencies (in-degree 0)
    levels: List[Set[str]] = []
    current_level: Set[str] = {node for node in all_nodes if in_degree[node] == 0}

    if not current_level and all_nodes:
        raise ValueError("Cycle detected: no nodes with zero in-degree")

    processed_count = 0

    while current_level:
        levels.append(current_level)
        processed_count += len(current_level)

        next_level: Set[str] = set()

        for node in current_level:
            for dependent in dependents[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    next_level.add(dependent)

        current_level = next_level

    # Check for cycle
    if processed_count != len(all_nodes):
        remaining = all_nodes - {n for level in levels for n in level}
        raise ValueError(f"Cycle detected involving nodes: {remaining}")

    return levels


def get_nodes_to_skip(dag: Dict[str, List[str]], failed_nodes: Set[str]) -> Set[str]:
    """
    Determine which nodes should be skipped due to failed dependencies.

    Uses BFS to find all transitive dependents of failed nodes.

    Args:
        dag: The DAG structure
        failed_nodes: Set of nodes that have failed

    Returns:
        Set of nodes that should be skipped
    """
    if not failed_nodes:
        return set()

    # Build reverse adjacency (dependents map)
    dependents: Dict[str, Set[str]] = defaultdict(set)
    for node, dependencies in dag.items():
        for dep in dependencies:
            dependents[dep].add(node)

    # BFS from all failed nodes
    to_skip: Set[str] = set()
    queue = deque(failed_nodes)

    while queue:
        current = queue.popleft()
        for dependent in dependents[current]:
            if dependent not in to_skip and dependent not in failed_nodes:
                to_skip.add(dependent)
                queue.append(dependent)

    return to_skip


def should_execute_node(
    node: str,
    dag: Dict[str, List[str]],
    completed_nodes: Set[str],
    failed_nodes: Set[str],
    skipped_nodes: Set[str]
) -> bool:
    """
    Check if a node should be executed.

    A node should execute if all its dependencies are in completed_nodes.
    """
    if node in skipped_nodes or node in failed_nodes:
        return False

    dependencies = dag.get(node, [])
    for dep in dependencies:
        if dep in failed_nodes or dep in skipped_nodes:
            return False
        if dep not in completed_nodes:
            return False

    return True


# =============================================================================
# Execution Functions
# =============================================================================

async def execute_single_node(
    client: TestZeusClient,
    step_name: str,
    test_id: str,
    execution_mode: str,
    poll_interval: float,
    timeout: float = 3600.0,
    test_env: Optional[str] = None,
    notification_channels: Optional[List[str]] = None
) -> NodeResult:
    """
    Execute a single test node and wait for completion.

    Args:
        client: Authenticated TestZeus client
        step_name: Name of the DAG step
        test_id: TestZeus test ID to execute
        execution_mode: "lenient" or "strict"
        poll_interval: Seconds between status polls
        timeout: Maximum wait time in seconds

    Returns:
        NodeResult with execution details
    """
    result = NodeResult(
        step_name=step_name,
        test_id=test_id,
        status=NodeStatus.RUNNING,
        start_time=datetime.now()
    )

    try:
        # Create and execute test run group with single test
        group_name = f"dag-{step_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        group = await client.test_run_groups.create_and_execute(
            name=group_name,
            test_ids=[test_id],
            execution_mode=execution_mode,
            environment=test_env,
            notification_channels=notification_channels
        )
        result.group_id = str(group.id)
        print(f"  [STARTED] {step_name} -> group_id: {group.id}")

        # Poll for completion
        elapsed = 0.0
        while elapsed < timeout:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            group = await client.test_run_groups.get_one(group.id)
            result.raw_status = group.status

            if group.is_completed():
                result.status = NodeStatus.COMPLETED
                result.end_time = datetime.now()
                return result

            if group.is_failed() or group.is_crashed():
                result.status = NodeStatus.FAILED
                result.end_time = datetime.now()
                result.error_message = f"Test run group status: {group.status}"
                return result

            if group.is_cancelled():
                result.status = NodeStatus.FAILED
                result.end_time = datetime.now()
                result.error_message = "Test run was cancelled"
                return result

        # Timeout
        result.status = NodeStatus.FAILED
        result.end_time = datetime.now()
        result.error_message = f"Timeout after {timeout} seconds"
        return result

    except Exception as e:
        result.status = NodeStatus.FAILED
        result.end_time = datetime.now()
        result.error_message = str(e)
        return result


async def execute_dag(
    client: TestZeusClient,
    test_map: Dict[str, str],
    dag: Dict[str, List[str]],
    execution_mode: str,
    poll_interval: float,
    node_timeout: float = 3600.0,
    test_env: Optional[str] = None,
    notification_channels: Optional[List[str]] = None
) -> DAGExecutionState:
    """
    Execute all tests according to DAG dependencies.

    Implements:
    1. Level-based parallel execution
    2. Fail-fast: skip dependent nodes on failure
    3. Continue independent branches

    Args:
        client: Authenticated TestZeus client
        test_map: Mapping of step names to test IDs
        dag: DAG structure (step -> dependencies)
        execution_mode: "lenient" or "strict"
        poll_interval: Polling interval for status checks
        node_timeout: Timeout per node execution

    Returns:
        DAGExecutionState with all results
    """
    # Initialize state
    state = DAGExecutionState(test_map=test_map, dag=dag)
    state.levels = compute_execution_levels(dag)

    print(f"\n[DAG] Computed {len(state.levels)} execution levels:")
    for i, level in enumerate(state.levels):
        print(f"  Level {i}: {sorted(level)}")

    # Execute level by level
    for level_idx, level_nodes in enumerate(state.levels):
        print(f"\n{'='*60}")
        print(f"[DAG] Executing Level {level_idx}")
        print(f"{'='*60}")

        # Determine which nodes in this level should run
        nodes_to_run: List[str] = []
        for node in level_nodes:
            if should_execute_node(node, dag, state.completed_nodes,
                                   state.failed_nodes, state.skipped_nodes):
                nodes_to_run.append(node)
            else:
                # Mark as skipped due to failed dependency
                state.skipped_nodes.add(node)
                state.node_results[node] = NodeResult(
                    step_name=node,
                    test_id=test_map[node],
                    status=NodeStatus.SKIPPED,
                    error_message="Skipped due to failed dependency"
                )
                print(f"  [SKIPPED] {node} - dependency failed")

        if not nodes_to_run:
            print(f"  No nodes to run in level {level_idx}")
            continue

        print(f"  Running {len(nodes_to_run)} node(s) in parallel: {nodes_to_run}")

        # Execute all runnable nodes in this level concurrently
        tasks = [
            execute_single_node(
                client=client,
                step_name=node,
                test_id=test_map[node],
                execution_mode=execution_mode,
                poll_interval=poll_interval,
                timeout=node_timeout,
                test_env=test_env,
                notification_channels=notification_channels
            )
            for node in nodes_to_run
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for node, result in zip(nodes_to_run, results):
            if isinstance(result, Exception):
                node_result = NodeResult(
                    step_name=node,
                    test_id=test_map[node],
                    status=NodeStatus.FAILED,
                    error_message=str(result),
                    end_time=datetime.now()
                )
                state.failed_nodes.add(node)
                print(f"  [FAILED] {node}: {result}")
            else:
                node_result = result
                if result.status == NodeStatus.COMPLETED:
                    state.completed_nodes.add(node)
                    print(f"  [COMPLETED] {node}")
                else:
                    state.failed_nodes.add(node)
                    print(f"  [FAILED] {node}: {result.error_message}")

            state.node_results[node] = node_result

        # After level completion, calculate newly skipped nodes
        newly_skipped = get_nodes_to_skip(dag, state.failed_nodes) - state.skipped_nodes
        if newly_skipped:
            print(f"  Nodes to skip due to failures: {newly_skipped}")
        state.skipped_nodes.update(newly_skipped)

    return state


# =============================================================================
# Output Functions
# =============================================================================

def generate_summary(state: DAGExecutionState, start_time: datetime) -> Dict[str, Any]:
    """Generate execution summary from DAG state."""
    end_time = datetime.now()

    completed = len(state.completed_nodes)
    failed = len(state.failed_nodes)
    skipped = len(state.skipped_nodes)
    total = len(state.test_map)

    # Determine overall status
    if failed == 0 and skipped == 0:
        overall_status = "success"
    elif completed > 0:
        overall_status = "partial_failure"
    else:
        overall_status = "failure"

    # Build node details
    node_details = []
    for step_name in state.test_map.keys():
        result = state.node_results.get(step_name)
        if result:
            node_details.append({
                "step": step_name,
                "test_id": result.test_id,
                "status": result.status.value,
                "group_id": result.group_id,
                "start_time": result.start_time.isoformat() if result.start_time else None,
                "end_time": result.end_time.isoformat() if result.end_time else None,
                "error": result.error_message,
                "raw_status": result.raw_status
            })

    return {
        "overall_status": overall_status,
        "total_nodes": total,
        "completed": completed,
        "failed": failed,
        "skipped": skipped,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds(),
        "execution_order": [list(level) for level in state.levels],
        "nodes": node_details
    }


def print_console_output(summary: Dict[str, Any]) -> None:
    """Print human-readable console output."""
    print("\n" + "=" * 60)
    print("DAG EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Overall Status: {summary['overall_status'].upper()}")
    print(f"Duration: {summary['duration_seconds']:.2f} seconds")
    print(f"Total Nodes: {summary['total_nodes']}")
    print(f"  - Completed: {summary['completed']}")
    print(f"  - Failed: {summary['failed']}")
    print(f"  - Skipped: {summary['skipped']}")
    print("-" * 60)
    print("Execution Order (by level):")
    for i, level in enumerate(summary['execution_order']):
        print(f"  Level {i}: {', '.join(sorted(level))}")
    print("-" * 60)
    print("Node Details:")
    for node in summary['nodes']:
        status_icon = {
            "completed": "[OK]  ",
            "failed": "[FAIL]",
            "crashed": "[CRASH]",
            "cancelled": "[CANCEL]",
            "skipped": "[SKIP]",
            "pending": "[...]",
            "running": "[RUN] "
        }.get(node["status"], "[?]   ")
        print(f"  {status_icon} {node['step']}: {node['status']}")
        if node.get("error"):
            print(f"         Error: {node['error']}")
    print("=" * 60)


def write_json_output(summary: Dict[str, Any], output_path: str) -> None:
    """Write JSON summary to file."""
    with open(output_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\n[OUTPUT] JSON summary written to: {output_path}")


# =============================================================================
# Main Entry Point
# =============================================================================

def get_credentials(args: argparse.Namespace) -> Tuple[str, str, Optional[str]]:
    """
    Get credentials from CLI args or environment variables.

    Args:
        args: Parsed command line arguments

    Returns:
        Tuple of (email, password, base_url)

    Raises:
        ValueError: If credentials are not provided
    """
    email = args.email or os.environ.get("TESTZEUS_EMAIL")
    password = args.password or os.environ.get("TESTZEUS_PASSWORD")
    base_url = args.base_url or "https://pb.prod.testzeus.app"

    if not email:
        raise ValueError(
            "Email not provided. Use --email argument or set TESTZEUS_EMAIL environment variable."
        )
    if not password:
        raise ValueError(
            "Password not provided. Use --password argument or set TESTZEUS_PASSWORD environment variable."
        )

    return email, password, base_url


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load and validate configuration from JSON file.

    Args:
        config_path: Path to JSON configuration file

    Returns:
        Config dict with keys: test_map, dag, and optional test_env, notification_channels

    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, 'r') as f:
        config = json.load(f)

    test_map = config.get("test_map")
    dag = config.get("dag")

    if not test_map:
        raise ValueError("Configuration must contain 'test_map'")
    if not dag:
        raise ValueError("Configuration must contain 'dag'")

    validate_dag(test_map, dag)

    return {
        "test_map": test_map,
        "dag": dag,
        "test_env": config.get("test_env"),
        "notification_channels": config.get("notification_channels"),
    }


async def run(args: argparse.Namespace) -> int:
    """
    Main execution flow.

    Returns:
        Exit code (0=success, 1=partial_failure, 2=failure)
    """
    start_time = datetime.now()
    print(f"[DAG] Starting execution at {start_time.isoformat()}")

    # Step 1: Read credentials
    print("\n[STEP 1] Reading credentials...")
    email, password, base_url = get_credentials(args)
    print(f"  Email: {email}")
    print(f"  Base URL: {base_url or '(default)'}")

    # Step 2: Login to TestZeus
    print("\n[STEP 2] Logging in to TestZeus...")
    client = TestZeusClient(
        email=email,
        password=password,
        base_url=base_url
    )

    try:
        async with client:
            await client.authenticate(email, password)
            print("  Login successful!")

            # Step 3: Read JSON config
            print("\n[STEP 3] Loading configuration...")
            config = load_config(args.config)
            test_map = config["test_map"]
            dag = config["dag"]
            print(f"  Loaded {len(test_map)} steps from {args.config}")

            # CLI args override config JSON values
            test_env = args.test_env or config.get("test_env")
            notification_channels = args.notification_channels or config.get("notification_channels")

            if test_env:
                print(f"  Test environment: {test_env}")
            if notification_channels:
                print(f"  Notification channels: {notification_channels}")

            # Step 4: Execute based on DAG definition
            print("\n[STEP 4] Executing DAG...")
            state = await execute_dag(
                client=client,
                test_map=test_map,
                dag=dag,
                execution_mode=args.execution_mode,
                poll_interval=args.poll_interval,
                node_timeout=args.timeout,
                test_env=test_env,
                notification_channels=notification_channels
            )

            # Step 5: Collect results
            print("\n[STEP 5] Collecting results...")
            summary = generate_summary(state, start_time)

            # Step 6: Output results
            print("\n[STEP 6] Writing output...")
            print_console_output(summary)
            write_json_output(summary, args.output)

            # Step 7: Logout
            print("\n[STEP 7] Logging out...")
            client.logout()
            print("  Logged out successfully!")

            # Return appropriate exit code
            if summary['overall_status'] == 'success':
                return 0
            elif summary['overall_status'] == 'partial_failure':
                return 1
            else:
                return 2

    except Exception as e:
        print(f"\n[ERROR] {e}")
        # Try to logout even on error
        try:
            client.logout()
        except Exception:
            pass
        return 2


def main() -> int:
    """Parse arguments and run the DAG test runner."""
    parser = argparse.ArgumentParser(
        description="Execute TestZeus tests based on a DAG structure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic usage with credentials file
    python dag_runner.pyz --config dag_config.json --credentials-file credentials.txt

    # With test environment
    python dag_runner.pyz --config dag_config.json --credentials-file credentials.txt \\
        --test-env <environment-id>

    # With notification channels
    python dag_runner.pyz --config dag_config.json --credentials-file credentials.txt \\
        --notification-channels <channel-id-1> <channel-id-2>

    # With all optional params
    python dag_runner.pyz --config dag_config.json --credentials-file credentials.txt \\
        --test-env <environment-id> \\
        --notification-channels <channel-id-1> <channel-id-2> \\
        --poll-interval 15 \\
        --output results.json
        """
    )

    parser.add_argument(
        "-c", "--config",
        required=True,
        help="Path to JSON configuration file with test_map and dag"
    )
    parser.add_argument(
        "-o", "--output",
        default="dag_results.json",
        help="Output JSON file path (default: dag_results.json)"
    )
    parser.add_argument(
        "--credentials-file",
        help="Path to credentials file (TESTZEUS_EMAIL, TESTZEUS_PASSWORD). Example: credentials.txt"
    )
    parser.add_argument(
        "--email",
        help="TestZeus email (or set TESTZEUS_EMAIL in credentials.txt or use --credentials-file)"
    )
    parser.add_argument(
        "--password",
        help="TestZeus password (or set TESTZEUS_PASSWORD in credentials.txt or use --credentials-file)"
    )
    parser.add_argument(
        "--base-url",
        help="TestZeus API base URL (default: https://pb.prod.testzeus.app)"
    )
    parser.add_argument(
        "--execution-mode",
        default="lenient",
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=30.0,
        help="Seconds between status polls (default: 30)"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3600.0,
        help="Timeout per node in seconds (default: 3600)"
    )
    parser.add_argument(
        "--test-env",
        help="Test environment ID to assign to each test run group (optional)"
    )
    parser.add_argument(
        "--notification-channels",
        nargs="+",
        help="List of notification channel IDs to add to each test run group (optional)"
    )

    args = parser.parse_args()

    # Load credentials file if specified, otherwise try default credentials.txt in current directory
    if args.credentials_file:
        env_path = Path(args.credentials_file)
        if not env_path.exists():
            print(f"[ERROR] Credentials file not found: {args.credentials_file}")
            return 2
        load_dotenv(env_path)
        print(f"[INFO] Loaded credentials from: {args.credentials_file}")
    else:
        # Try to load credentials.txt from current directory (silent if not found)
        load_dotenv("credentials.txt")

    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
