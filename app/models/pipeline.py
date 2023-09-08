from sqlalchemy import String, Integer, Text, Boolean, ForeignKey, Enum as SAEnum
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, relationship
from sqlalchemy.sql import func
from sqlalchemy import DateTime
import enum
from datetime import datetime

# SQLAlchemy 2.0 mapped_column / Mapped — much better than 1.4 Column() style.
# Type inference from Mapped[str] means no more nullable=True/False guessing.
# Pydantic V2 note: orm_mode=True is now model_config = ConfigDict(from_attributes=True).
# Took me a day to figure out why pydantic.from_orm() was broken after upgrading.


class Base(DeclarativeBase):
    pass


class PipelineStatus(str, enum.Enum):
    ACTIVE   = "active"
    PAUSED   = "paused"
    ARCHIVED = "archived"


class ExecutionStatus(str, enum.Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILED    = "failed"
    CANCELLED = "cancelled"


class StepType(str, enum.Enum):
    HTTP_FETCH   = "http_fetch"
    SQL_QUERY    = "sql_query"
    TRANSFORM    = "transform"
    HTTP_WEBHOOK = "http_webhook"
    FILE_EXPORT  = "file_export"


class Pipeline(Base):
    __tablename__ = "pipelines"

    id:          Mapped[int]      = mapped_column(Integer, primary_key=True)
    name:        Mapped[str]      = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status:      Mapped[PipelineStatus] = mapped_column(
        SAEnum(PipelineStatus), default=PipelineStatus.ACTIVE
    )
    schedule:    Mapped[str | None] = mapped_column(String(100), nullable=True)  # cron expr
    owner_id:    Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at:  Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at:  Mapped[datetime | None] = mapped_column(DateTime, onupdate=func.now())

    steps:      Mapped[list["PipelineStep"]]     = relationship("PipelineStep", back_populates="pipeline", order_by="PipelineStep.position")
    executions: Mapped[list["PipelineExecution"]] = relationship("PipelineExecution", back_populates="pipeline")


class PipelineStep(Base):
    __tablename__ = "pipeline_steps"

    id:          Mapped[int]      = mapped_column(Integer, primary_key=True)
    pipeline_id: Mapped[int]      = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    position:    Mapped[int]      = mapped_column(Integer, nullable=False)
    name:        Mapped[str]      = mapped_column(String(255), nullable=False)
    step_type:   Mapped[StepType] = mapped_column(SAEnum(StepType), nullable=False)
    config:      Mapped[str]      = mapped_column(Text, default="{}")  # JSON config
    timeout_seconds: Mapped[int]  = mapped_column(Integer, default=300)
    retry_count:     Mapped[int]  = mapped_column(Integer, default=0)
    enabled:         Mapped[bool] = mapped_column(Boolean, default=True)

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="steps")


class PipelineExecution(Base):
    __tablename__ = "pipeline_executions"

    id:          Mapped[int]           = mapped_column(Integer, primary_key=True)
    pipeline_id: Mapped[int]           = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    status:      Mapped[ExecutionStatus] = mapped_column(
        SAEnum(ExecutionStatus), default=ExecutionStatus.PENDING
    )
    triggered_by: Mapped[str]    = mapped_column(String(50), default="manual")  # manual|scheduler|api
    started_at:   Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at:  Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error:        Mapped[str | None]      = mapped_column(Text, nullable=True)
    celery_task_id: Mapped[str | None]   = mapped_column(String(255), nullable=True)
    created_at:   Mapped[datetime]        = mapped_column(DateTime, server_default=func.now())

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="executions")
    step_results: Mapped[list["StepResult"]] = relationship("StepResult", back_populates="execution")


class StepResult(Base):
    __tablename__ = "step_results"

    id:           Mapped[int]           = mapped_column(Integer, primary_key=True)
    execution_id: Mapped[int]           = mapped_column(ForeignKey("pipeline_executions.id"), nullable=False)
    step_id:      Mapped[int]           = mapped_column(ForeignKey("pipeline_steps.id"), nullable=False)
    status:       Mapped[ExecutionStatus] = mapped_column(SAEnum(ExecutionStatus), nullable=False)
    output:       Mapped[str | None]    = mapped_column(Text, nullable=True)  # JSON
    error:        Mapped[str | None]    = mapped_column(Text, nullable=True)
    duration_ms:  Mapped[int | None]    = mapped_column(Integer, nullable=True)
    started_at:   Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    execution: Mapped["PipelineExecution"] = relationship("PipelineExecution", back_populates="step_results")
