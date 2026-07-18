from collections.abc import Callable
from dataclasses import dataclass

import jmespath
from expression import Result

from shared.customtypes import Error
from shared.pipeline.actionhandler import DataDto
from shared.utils.parse import NonEmptyStr


# ============================================================================
# ProjectionError
# ============================================================================

@dataclass(frozen=True)
class ProjectionError(Error):
    """
    Error type for projection-related failures.
    
    Common error messages:
      - "invalid 'projection' value: <details>"
      - "projection returned None (field may be missing or value is null)"
      - "JMESPath runtime error: <details>"
    """
    pass


# ============================================================================
# ProjectionResolver — value object
# ============================================================================

class ProjectionResolver:
    """
    Applies JMESPath projection to DataDto.
    
    Responsibilities:
      - Compile JMESPath expression once (performance optimization)
      - Apply projection to single DataDto
      - Handle identity mode (no projection)
      - Distinguish missing fields from legitimate null values
      - Return Result for safe error handling
    
    Key behaviors:
      - If projection is None, returns original DataDto unchanged (identity mode)
      - If projection returns None, returns Result.Error (fail-fast)
      - If projection returns non-dict, wraps it in {"value": result} for uniform comparison
      - JMESPath runtime errors are caught and returned as Result.Error
    
    Immutability:
      - Once created, ProjectionResolver cannot be modified
      - Safe to share across multiple strategy invocations
      - Thread-safe for read-only operations
    """
    
    def __init__(self, compiled: jmespath.parser.ParsedResult | None) -> None:
        """
        Initialize ProjectionResolver with a compiled JMESPath expression.
        
        Args:
            compiled: Pre-compiled JMESPath query, or None for identity mode.
                      Use create_projection_resolver() to construct safely.
        """
        self._compiled = compiled
    
    def apply(self, dto: DataDto) -> Result[DataDto, ProjectionError]:
        """
        Apply projection to a single DataDto.
        
        Args:
            dto: Input data dictionary to project.
        
        Returns:
            Result.Ok(projection) on success:
              - If identity mode: returns original dto unchanged
              - If projection returns dict: returns as-is
              - If projection returns scalar/list: wraps in {"value": scalar/list}
            Result.Error(ProjectionError) on failure:
              - If projection returns None (field missing or null value)
              - If JMESPath runtime error occurs
        
        Notes:
          - None is treated as an error (fail-fast semantics)
          - For legitimate null handling, use JMESPath expressions like:
            field || 'default' or [field, 'default'] | [0]
          - Scalar/list results are wrapped to ensure uniform comparison
        """
        # Identity mode: return original dto
        if self._compiled is None:
            return Result.Ok(dto)
        
        # Apply JMESPath with error handling
        try:
            result = self._compiled.search(dto)
        except Exception as e:
            error = ProjectionError(f"JMESPath runtime error: {e}")
            return Result.Error(error)
        
        # None is treated as error (missing field or null value)
        if result is None:
            error = ProjectionError("projection returned None (field may be missing or value is null)")
            return Result.Error(error)
        
        # Wrap scalars for uniform comparison
        if not isinstance(result, dict):
            result = {"value": result}
        
        return Result.Ok(result)


# ============================================================================
# ProjectionResolverCreator — factory-function to create ProjectionResolver
# ============================================================================

type ProjectionResolverCreator = Callable[[NonEmptyStr | None], Result[ProjectionResolver, str]]


def create_projection_resolver(
    projection: NonEmptyStr | None
) -> Result[ProjectionResolver, ProjectionError]:
    """
    Create a ProjectionResolver from a JMESPath expression string.
    
    It validates JMESPath syntax at construction time (fail-fast).
    
    Args:
        projection: JMESPath expression string, or None for identity mode.
    
    Returns:
        Result.Ok(ProjectionResolver) on success:
          - If projection is None: returns identity resolver
          - If projection is valid: returns compiled resolver
        Result.Error(ProjectionError) on failure:
          - If JMESPath syntax is invalid
    
    Notes:
      - Never raises exceptions; all errors returned via Result
      - Syntax validation occurs here, not at runtime
      - Runtime errors (missing fields) handled in ProjectionResolver.apply()
    
    Examples:
      >>> create_projection_resolver(None)
      Result.Ok(ProjectionResolver(identity))
      
      >>> create_projection_resolver("user.name")
      Result.Ok(ProjectionResolver(compiled))
      
      >>> create_projection_resolver("invalid[[[syntax")
      Result.Error(ProjectionError("invalid 'projection' value: ..."))
    """
    # Identity mode
    if projection is None:
        return Result.Ok(ProjectionResolver(None))
    
    # Compile JMESPath with error handling
    try:
        compiled = jmespath.compile(projection)
        return Result.Ok(ProjectionResolver(compiled))
    except Exception as e:
        error = ProjectionError(f"invalid 'projection' value: {e}")
        return Result.Error(error)