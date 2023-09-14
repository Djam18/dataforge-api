from fastapi import APIRouter, HTTPException, Depends, status, BackgroundTasks
from pydantic import BaseModel, ConfigDict  # Pydantic V2
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.pipeline import Pipeline, PipelineStep, PipelineStatus, StepType
from typing import Optional
import json

# Pydantic V2 — breaking change from previous FastAPI project.
# orm_mode=True is now model_config = ConfigDict(from_attributes=True).
# field_validator replaces @validator (different signature too).
# Took a solid afternoon to migrate. Worth it — V2 is ~5-10x faster validation.

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


# ──────────────────────────────────────────
#  Pydantic V2 schemas
# ──────────────────────────────────────────

class StepConfigSchema(BaseModel):
    url:     Optional[str] = None    # http_fetch / http_webhook
    method:  Optional[str] = "GET"
    headers: Optional[dict] = None
    query:   Optional[str] = None    # sql_query
    transform_fn: Optional[str] = None  # python expression string


class StepCreate(BaseModel):
    name:            str
    step_type:       StepType
    config:          StepConfigSchema = StepConfigSchema()
    position:        int = 0
    timeout_seconds: int = 300
    retry_count:     int = 0


class PipelineCreate(BaseModel):
    name:        str
    description: Optional[str] = None
    schedule:    Optional[str] = None  # cron expression: "0 2 * * *"
    steps:       list[StepCreate] = []


class StepResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)  # Pydantic V2 — replaces class Config: orm_mode=True

    id:              int
    name:            str
    step_type:       StepType
    position:        int
    timeout_seconds: int
    retry_count:     int
    enabled:         bool


class PipelineResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:          int
    name:        str
    description: Optional[str]
    status:      PipelineStatus
    schedule:    Optional[str]
    steps:       list[StepResponse] = []


class PipelineUpdate(BaseModel):
    name:        Optional[str] = None
    description: Optional[str] = None
    schedule:    Optional[str] = None
    status:      Optional[PipelineStatus] = None


# ──────────────────────────────────────────
#  Dependency
# ──────────────────────────────────────────

async def get_db():
    """Yield DB session — injected via Depends()."""
    from app.database import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        yield session


# ──────────────────────────────────────────
#  Routes
# ──────────────────────────────────────────

@router.post("/", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED)
async def create_pipeline(body: PipelineCreate, db: AsyncSession = Depends(get_db)):
    pipeline = Pipeline(
        name=body.name,
        description=body.description,
        schedule=body.schedule,
    )
    db.add(pipeline)
    await db.flush()  # get pipeline.id before adding steps

    for i, step_data in enumerate(body.steps):
        step = PipelineStep(
            pipeline_id=pipeline.id,
            name=step_data.name,
            step_type=step_data.step_type,
            config=json.dumps(step_data.config.model_dump()),  # Pydantic V2: .dict() → .model_dump()
            position=step_data.position or i,
            timeout_seconds=step_data.timeout_seconds,
            retry_count=step_data.retry_count,
        )
        db.add(step)

    await db.commit()
    await db.refresh(pipeline)
    return pipeline


@router.get("/", response_model=list[PipelineResponse])
async def list_pipelines(
    status: Optional[PipelineStatus] = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(Pipeline)
    if status:
        q = q.where(Pipeline.status == status)
    result = await db.execute(q.order_by(Pipeline.created_at.desc()))
    return result.scalars().all()


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(pipeline_id: int, db: AsyncSession = Depends(get_db)):
    pipeline = await db.get(Pipeline, pipeline_id)
    if not pipeline:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "pipeline not found")
    return pipeline


@router.patch("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: int,
    body: PipelineUpdate,
    db: AsyncSession = Depends(get_db),
):
    pipeline = await db.get(Pipeline, pipeline_id)
    if not pipeline:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "pipeline not found")

    # Pydantic V2: .dict(exclude_unset=True) → .model_dump(exclude_unset=True)
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(pipeline, field, value)

    await db.commit()
    await db.refresh(pipeline)
    return pipeline


@router.delete("/{pipeline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pipeline(pipeline_id: int, db: AsyncSession = Depends(get_db)):
    pipeline = await db.get(Pipeline, pipeline_id)
    if not pipeline:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "pipeline not found")
    await db.delete(pipeline)
    await db.commit()
