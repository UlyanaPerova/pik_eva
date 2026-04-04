"""
Структурированный результат запуска парсера.

Используется всеми runners для стандартизированного возврата:
  - success/failure
  - количество items
  - ошибки/предупреждения
  - длительность
  - путь к выходному файлу

Для GUI/оркестранта: единый интерфейс результата вместо голого exit code.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunResult:
    """Результат запуска одного парсера."""

    success: bool
    site: str
    items_count: int = 0
    items_saved: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    duration_sec: float = 0.0
    output_path: str | None = None

    @property
    def exit_code(self) -> int:
        """Обратная совместимость: 0 = success, 1 = failure."""
        return 0 if self.success else 1

    def summary(self) -> str:
        """Краткая строка для лога."""
        status = "OK" if self.success else "FAIL"
        parts = [
            f"[{status}] {self.site}",
            f"items={self.items_count}",
            f"saved={self.items_saved}",
            f"{self.duration_sec:.1f}s",
        ]
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        if self.warnings:
            parts.append(f"warnings={len(self.warnings)}")
        return " | ".join(parts)
