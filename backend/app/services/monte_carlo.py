"""
Monte Carlo simulation orchestrator.

Why this exists
---------------
A single OASIS rollout tells you almost nothing about what's *robust*. LLM-
driven agents are stochastic — same config, different run, different result.
The single most important epistemic improvement we can make is to run the
same prepared simulation N times and report what's stable vs. what varies.

How it works
------------
Given a prepared simulation_id (one that has run through prepare_simulation
to completion — graph, profiles, config all on disk), `start_runs(sim_id, N)`
spawns N OASIS subprocesses in parallel, each in its own subdirectory:

    uploads/simulations/<sim_id>/mc/run_0/
                                 /run_1/
                                 ...
                                 /run_<N-1>/

Each run gets a copy (or symlink) of the prepared artifacts:
    - simulation_config.json
    - reddit_profiles.json (if Reddit enabled)
    - twitter_profiles.csv (if Twitter enabled)

Each subprocess writes its own action stream to
    <run_dir>/twitter/actions.jsonl  and/or
    <run_dir>/reddit/actions.jsonl

A monitor thread tails each file and writes structured rows into the shim's
agent_actions table tagged with both `simulation_id` and `run_id`. The
report agent can then query the variance directly via the existing
`query_actions` tool, filtering by run_id.

Scope intentionally small
-------------------------
This module does NOT replace SimulationRunner's single-run path. It runs in
parallel as a separate orchestrator, only used when the user explicitly
opts into Monte Carlo. No changes to existing simulation flows.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger("mirofish.monte_carlo")


SCRIPTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "scripts",
)
SIMULATIONS_ROOT = Config.OASIS_SIMULATION_DATA_DIR


@dataclass
class _RunHandle:
    run_id: str
    run_dir: str
    process: Optional[subprocess.Popen] = None
    monitor_thread: Optional[threading.Thread] = None
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    last_position: Dict[str, int] = field(default_factory=dict)  # log_path -> byte offset
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_dir": self.run_dir,
            "pid": self.process.pid if self.process else None,
            "alive": (self.process.poll() is None) if self.process else False,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }


class MonteCarloRunner:
    """Class-level registry, mirroring SimulationRunner's pattern."""

    # parent_sim_id -> {run_id: _RunHandle}
    _runs: Dict[str, Dict[str, _RunHandle]] = {}
    _lock = threading.RLock()

    # ------------------------------------------------------------------

    @classmethod
    def list_runs(cls, simulation_id: str) -> List[Dict[str, Any]]:
        with cls._lock:
            handles = cls._runs.get(simulation_id, {})
            return [h.to_dict() for h in handles.values()]

    @classmethod
    def is_running(cls, simulation_id: str) -> bool:
        with cls._lock:
            handles = cls._runs.get(simulation_id, {})
            return any(
                h.process is not None and h.process.poll() is None
                for h in handles.values()
            )

    # ------------------------------------------------------------------

    @classmethod
    def start_runs(
        cls,
        simulation_id: str,
        n_runs: int = 3,
        platform: str = "reddit",
        max_rounds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Spawn N parallel OASIS subprocesses for a prepared simulation.

        Returns immediately; runs continue in the background. Use list_runs()
        to observe.
        """
        if n_runs < 2:
            raise ValueError("Monte Carlo requires n_runs >= 2 to be meaningful")
        if n_runs > 10:
            raise ValueError("n_runs > 10 is wasteful; cap is 10")

        sim_dir = os.path.join(SIMULATIONS_ROOT, simulation_id)
        config_path = os.path.join(sim_dir, "simulation_config.json")
        if not os.path.isfile(config_path):
            raise FileNotFoundError(
                f"Simulation {simulation_id} is not prepared "
                f"(no simulation_config.json found)"
            )

        script_name = {
            "reddit": "run_reddit_simulation.py",
            "twitter": "run_twitter_simulation.py",
            "parallel": "run_parallel_simulation.py",
        }.get(platform)
        if not script_name:
            raise ValueError(f"Unknown platform: {platform}")
        script_path = os.path.join(SCRIPTS_DIR, script_name)
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Run script not found: {script_path}")

        if cls.is_running(simulation_id):
            raise RuntimeError(
                f"Monte Carlo already running for {simulation_id}; "
                f"call stop_runs() first or wait for it to finish."
            )

        mc_root = os.path.join(sim_dir, "mc")
        os.makedirs(mc_root, exist_ok=True)

        with cls._lock:
            cls._runs[simulation_id] = {}
            for i in range(n_runs):
                run_id = f"run_{i}"
                run_dir = os.path.join(mc_root, run_id)
                # Fresh dir for this run — clear any prior MC artifacts.
                if os.path.exists(run_dir):
                    shutil.rmtree(run_dir, ignore_errors=True)
                os.makedirs(run_dir, exist_ok=True)

                # Copy the prepared artifacts into the run dir so the OASIS
                # script (cwd=run_dir) finds them.
                for fname in (
                    "simulation_config.json",
                    "reddit_profiles.json",
                    "twitter_profiles.csv",
                    "agent_history.json",
                ):
                    src = os.path.join(sim_dir, fname)
                    if os.path.isfile(src):
                        shutil.copy2(src, os.path.join(run_dir, fname))

                handle = _RunHandle(run_id=run_id, run_dir=run_dir)
                cls._runs[simulation_id][run_id] = handle

                cls._spawn_one(
                    parent_sim_id=simulation_id,
                    handle=handle,
                    script_path=script_path,
                    config_path=os.path.join(run_dir, "simulation_config.json"),
                    max_rounds=max_rounds,
                    platform=platform,
                )

        logger.info(
            f"Monte Carlo started: sim={simulation_id} runs={n_runs} platform={platform}"
        )
        return {
            "simulation_id": simulation_id,
            "n_runs": n_runs,
            "platform": platform,
            "runs": cls.list_runs(simulation_id),
        }

    # ------------------------------------------------------------------

    @classmethod
    def _spawn_one(
        cls,
        parent_sim_id: str,
        handle: _RunHandle,
        script_path: str,
        config_path: str,
        max_rounds: Optional[int],
        platform: str,
    ) -> None:
        cmd = [sys.executable, script_path, "--config", config_path]
        if max_rounds is not None and max_rounds > 0:
            cmd.extend(["--max-rounds", str(max_rounds)])

        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        log_path = os.path.join(handle.run_dir, "simulation.log")
        log_file = open(log_path, "w", encoding="utf-8")

        process = subprocess.Popen(
            cmd,
            cwd=handle.run_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            bufsize=1,
            env=env,
            start_new_session=True,
        )
        handle.process = process

        monitor = threading.Thread(
            target=cls._monitor_one,
            args=(parent_sim_id, handle, platform),
            daemon=True,
        )
        monitor.start()
        handle.monitor_thread = monitor

        logger.info(
            f"MC run started: sim={parent_sim_id} run={handle.run_id} pid={process.pid}"
        )

    # ------------------------------------------------------------------

    @classmethod
    def _monitor_one(
        cls, parent_sim_id: str, handle: _RunHandle, platform: str
    ) -> None:
        """Tail the run's action.jsonl files and persist actions to the store."""
        try:
            from zep_cloud._storage import get_store
            store = get_store()
        except Exception as e:
            handle.error = f"storage unavailable: {e}"
            logger.error(handle.error)
            return

        # Both platforms might write — watch whichever exists.
        candidates = [
            os.path.join(handle.run_dir, "twitter", "actions.jsonl"),
            os.path.join(handle.run_dir, "reddit", "actions.jsonl"),
        ]

        proc = handle.process
        while proc is not None:
            for path in candidates:
                cls._consume_log(store, parent_sim_id, handle, path)
            ret = proc.poll()
            if ret is not None:
                # Process finished — drain one more time, then exit.
                for path in candidates:
                    cls._consume_log(store, parent_sim_id, handle, path)
                handle.finished_at = time.time()
                if ret != 0:
                    handle.error = f"subprocess exited with code {ret}"
                    logger.warning(
                        f"MC run {handle.run_id} (sim {parent_sim_id}) "
                        f"exited with code {ret}"
                    )
                else:
                    logger.info(
                        f"MC run {handle.run_id} (sim {parent_sim_id}) completed cleanly"
                    )
                return
            time.sleep(2)

    @classmethod
    def _consume_log(
        cls,
        store,
        parent_sim_id: str,
        handle: _RunHandle,
        log_path: str,
    ) -> None:
        if not os.path.exists(log_path):
            return
        platform = "twitter" if "/twitter/" in log_path else "reddit"
        position = handle.last_position.get(log_path, 0)
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                f.seek(position)
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        action_data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if "event_type" in action_data:
                        # round_end / simulation_end — not an agent action
                        continue
                    store.record_action(
                        simulation_id=parent_sim_id,
                        action_data=action_data,
                        platform=platform,
                        run_id=handle.run_id,
                    )
                handle.last_position[log_path] = f.tell()
        except Exception as e:
            logger.warning(f"MC log tail failed ({log_path}): {e}")

    # ------------------------------------------------------------------

    @classmethod
    def stop_runs(cls, simulation_id: str) -> Dict[str, Any]:
        """Best-effort kill of all subprocesses for this simulation."""
        with cls._lock:
            handles = cls._runs.get(simulation_id, {})
            killed = 0
            for h in handles.values():
                if h.process and h.process.poll() is None:
                    try:
                        os.killpg(os.getpgid(h.process.pid), 15)
                        killed += 1
                    except Exception as e:
                        logger.warning(f"Failed to kill {h.run_id}: {e}")
            return {"simulation_id": simulation_id, "killed": killed}

    # ------------------------------------------------------------------

    @classmethod
    def variance_summary(cls, simulation_id: str) -> Dict[str, Any]:
        """Compute per-run action rollups + cross-run variance signal.

        Returns a dict with one entry per run_id plus a `consensus` block:
            {
              "runs": {
                "run_0": {"total": 412, "by_action_type": {...}, "top_agents": [...]},
                "run_1": {...},
                ...
              },
              "consensus": {
                  "stable_action_types": ["LIKE_POST", "CREATE_POST"],   # appear in all runs
                  "variable_action_types": ["FOLLOW"],                    # appear in only some
                  "top_agents_intersection": ["alice", "bob"]             # active in every run
              }
            }
        """
        try:
            from zep_cloud._storage import get_store
            store = get_store()
        except Exception as e:
            return {"runs": {}, "consensus": {}, "error": str(e)}

        with cls._lock:
            handles = cls._runs.get(simulation_id) or {}
            run_ids = list(handles.keys())

        # If the runner has no record (e.g. server restarted), fall back to
        # whatever run_ids exist in the action log.
        if not run_ids:
            try:
                with store._lock, store._connect() as conn:  # type: ignore[attr-defined]
                    rows = conn.execute(
                        "SELECT DISTINCT run_id FROM agent_actions "
                        "WHERE simulation_id=? AND run_id IS NOT NULL",
                        (simulation_id,),
                    ).fetchall()
                run_ids = [r["run_id"] for r in rows if r["run_id"]]
            except Exception:
                run_ids = []

        per_run: Dict[str, Dict[str, Any]] = {}
        type_sets: List[set] = []
        agent_sets: List[set] = []
        for run_id in run_ids:
            summary = store.action_summary(simulation_id, run_id=run_id)
            per_run[run_id] = summary
            type_sets.append({r["type"] for r in summary.get("by_action_type", [])})
            agent_sets.append(
                {r["agent"] for r in summary.get("top_agents", []) if r["agent"]}
            )

        if type_sets:
            stable_types = sorted(set.intersection(*type_sets)) if len(type_sets) > 1 else sorted(type_sets[0])
            all_types = sorted(set.union(*type_sets))
            variable_types = [t for t in all_types if t not in stable_types]
        else:
            stable_types, variable_types = [], []

        if agent_sets:
            top_intersection = sorted(set.intersection(*agent_sets)) if len(agent_sets) > 1 else sorted(agent_sets[0])
        else:
            top_intersection = []

        return {
            "simulation_id": simulation_id,
            "n_runs": len(run_ids),
            "runs": per_run,
            "consensus": {
                "stable_action_types": stable_types,
                "variable_action_types": variable_types,
                "top_agents_intersection": top_intersection,
            },
        }
