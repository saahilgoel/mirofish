"""
QuickSim — one-shot sim creation from a plain-English question.

POST /api/quicksim
Body: {"question": str, "project_name"?: str}
Returns: {"success": true, "data": {"task_id": "...", "project_id": "..."}}

The task runs in the background and walks the normal chain:
    1. LLM → generate a grounded seed markdown
    2. LLM → generate a simulation requirement prompt
    3. Create project + save seed as its extracted text
    4. LLM → generate ontology
    5. Build graph (async via GraphBuilderService)
    6. Create simulation + trigger prepare

Progress is reported via TaskManager, so the frontend can poll
/api/graph/task/<task_id> the same way it polls other long-running tasks.
"""

import os
import threading
import traceback
from typing import Any, Dict, Optional

from flask import request, jsonify

from . import graph_bp
from ..config import Config
from ..models.project import ProjectManager, ProjectStatus
from ..models.task import TaskManager, TaskStatus
from ..services.graph_builder import GraphBuilderService
from ..services.ontology_generator import OntologyGenerator
from ..services.simulation_manager import SimulationManager
from ..services.text_processor import TextProcessor
from ..utils.llm_client import LLMClient
from ..utils.logger import get_logger

logger = get_logger('mirofish.quicksim')


# ---------- LLM prompts ------------------------------------------------------

_SEED_SYSTEM = """You write dense, grounded "seed documents" for population-scale
social simulations. The seed must be ~1500-3000 words of factual context that a
downstream LLM can use to build a realistic simulation of real people.

Output a markdown document with these sections:
  1. Population definition (who exactly, how many, where)
  2. Categories / sub-segments (with counts where reasonable)
  3. Current state & pain points (specific, grounded)
  4. Technology / tools in use today (real product names when known)
  5. Economic / revenue profile (ranges OK)
  6. Key personas (3-6 named archetypes with one-paragraph backstories)
  7. External forces (regulatory, competitive, macro) shaping behavior

Rules:
- Prefer concrete numbers and product names over generic claims
- If you don't know something precisely, give an explicit range and say so
- No bullet-point soup; use short prose where it helps
- No preamble, no conclusion — start directly with `# <title>`"""

_SIM_REQ_SYSTEM = """You write the *simulation requirement* prompt for a social
simulation. Given a user's question and a seed context document, emit a single
paragraph (~100-200 words) that:

  - Restates the specific question the simulation must answer
  - Names the population and time horizon
  - Lists 2-4 key behaviors/decisions the agents must reason about
  - Calls out what the simulation should *output* (adoption patterns, company
    moves, segment-level differences, etc.)

Plain text only, no headings, no bullets, no markdown. Start directly with the
requirement."""


# ---------- helper tasks -----------------------------------------------------


def _llm_generate(system: str, user: str, max_tokens: int = 4096) -> str:
    client = LLMClient()
    return client.chat(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.6,
        max_tokens=max_tokens,
    )


def _quicksim_worker(task_id: str, question: str, project_name: str) -> None:
    task_manager = TaskManager()
    project = None

    def tick(message: str, progress: int, extra: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = {"message": message, "progress": progress}
        if extra:
            payload["progress_detail"] = extra
        task_manager.update_task(task_id, **payload)

    try:
        tick("Generating grounded seed document...", 5)
        seed_md = _llm_generate(_SEED_SYSTEM, question, max_tokens=4096).strip()
        if not seed_md:
            raise RuntimeError("Seed generation returned empty output")

        tick("Writing simulation requirement prompt...", 20)
        sim_req = _llm_generate(
            _SIM_REQ_SYSTEM,
            f"QUESTION:\n{question}\n\nSEED CONTEXT:\n{seed_md[:4000]}",
            max_tokens=1024,
        ).strip()
        if not sim_req:
            raise RuntimeError("Simulation requirement generation returned empty output")

        tick("Creating project...", 25)
        project = ProjectManager.create_project(name=project_name)
        project.simulation_requirement = sim_req
        project.total_text_length = len(seed_md)
        project.files = [{"filename": "seed.md", "size": len(seed_md)}]
        ProjectManager.save_project(project)

        # Persist the seed both as the extracted-text blob (what graph build reads)
        # and as a seed.md file so the UI/files list sees it.
        ProjectManager.save_extracted_text(project.project_id, seed_md)
        try:
            files_dir = ProjectManager._get_project_files_dir(project.project_id)  # type: ignore[attr-defined]
            os.makedirs(files_dir, exist_ok=True)
            with open(os.path.join(files_dir, "seed.md"), "w", encoding="utf-8") as f:
                f.write(seed_md)
        except Exception as e:
            logger.warning(f"quicksim: failed to persist seed.md: {e}")

        tick("Generating ontology from seed...", 35)
        ontology = OntologyGenerator().generate(
            document_texts=[seed_md],
            simulation_requirement=sim_req,
        )
        project.ontology = ontology
        project.status = ProjectStatus.ONTOLOGY_GENERATED
        ProjectManager.save_project(project)

        tick("Building knowledge graph...", 55)
        builder = GraphBuilderService(api_key=Config.ZEP_API_KEY)
        chunks = TextProcessor.split_text(
            seed_md,
            chunk_size=project.chunk_size or Config.DEFAULT_CHUNK_SIZE,
            overlap=project.chunk_overlap or Config.DEFAULT_CHUNK_OVERLAP,
        )
        graph_id = builder.create_graph(name=project.name or "QuickSim Graph")
        project.graph_id = graph_id
        project.status = ProjectStatus.GRAPH_BUILDING
        ProjectManager.save_project(project)
        builder.set_ontology(graph_id, ontology)

        total_chunks = len(chunks) or 1

        def chunk_progress(msg: str, ratio: float) -> None:
            # 55% -> 85% reserved for chunk ingest
            tick(msg, 55 + int(ratio * 30))

        episode_uuids = builder.add_text_batches(
            graph_id, chunks, batch_size=3, progress_callback=chunk_progress
        )

        tick("Waiting for graph extraction to finish...", 85)
        builder._wait_for_episodes(episode_uuids, lambda m, r: tick(m, 85 + int(r * 5)))
        project.status = ProjectStatus.GRAPH_COMPLETED
        ProjectManager.save_project(project)

        tick("Creating simulation...", 92)
        sim_manager = SimulationManager()
        sim_state = sim_manager.create_simulation(
            project_id=project.project_id,
            graph_id=graph_id,
            enable_twitter=True,
            enable_reddit=True,
        )

        tick("Kicking off simulation preparation...", 95)
        # Run prepare async in this same thread (it's already a background
        # thread) so the QuickSim task completes only when prep is done.
        sim_manager.prepare_simulation(
            simulation_id=sim_state.simulation_id,
            simulation_requirement=sim_req,
            document_text=seed_md,
            progress_callback=lambda stage, pct, msg, **_kw: tick(
                f"Preparing: {stage} — {msg}", 95 + int(pct * 0.05)
            ),
        )

        task_manager.update_task(
            task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            message="QuickSim ready",
            result={
                "project_id": project.project_id,
                "graph_id": graph_id,
                "simulation_id": sim_state.simulation_id,
                "seed_preview": seed_md[:500],
                "simulation_requirement": sim_req,
            },
        )
        logger.info(
            f"QuickSim complete: project={project.project_id}, "
            f"sim={sim_state.simulation_id}"
        )
    except Exception as e:
        logger.error(f"QuickSim failed: {e}")
        logger.error(traceback.format_exc())
        if project:
            try:
                project.status = ProjectStatus.FAILED
                project.error = str(e)
                ProjectManager.save_project(project)
            except Exception:
                pass
        task_manager.update_task(
            task_id,
            status=TaskStatus.FAILED,
            message=f"QuickSim failed: {e}",
            error=traceback.format_exc(),
        )


# ---------- route ------------------------------------------------------------


@graph_bp.route('/quicksim', methods=['POST'])
def quicksim():
    data = request.get_json(silent=True) or {}
    question = (data.get('question') or '').strip()
    project_name = (data.get('project_name') or 'QuickSim').strip() or 'QuickSim'

    if not question:
        return jsonify({"success": False, "error": "`question` is required"}), 400
    if len(question) > 4000:
        return jsonify({"success": False, "error": "question too long (max 4000 chars)"}), 400

    task_manager = TaskManager()
    task_id = task_manager.create_task(f"QuickSim: {project_name}")
    task_manager.update_task(task_id, status=TaskStatus.PROCESSING, progress=0,
                             message="Starting QuickSim...")

    thread = threading.Thread(
        target=_quicksim_worker, args=(task_id, question, project_name), daemon=True
    )
    thread.start()

    return jsonify({
        "success": True,
        "data": {
            "task_id": task_id,
            "message": "QuickSim task started. Poll /api/graph/task/<task_id> for progress.",
        },
    })
