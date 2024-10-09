import logging
from typing import Any, List, Mapping, Optional

from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models.llms import LLM
from snowflake.snowpark.session import Session

logger = logging.getLogger(__name__)


class SQLCortex(LLM):
    session: Session = None

    model: str = "llama3.1-405b"

    @property
    def _llm_type(self) -> str:
        return "sqlcortex"

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        if stop is not None:
            raise ValueError("stop kwargs are not permitted.")
        q = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('{self.model}', $$%(prompt)s$$) as COMPLETION"""
        res = self.session.sql(q % {"prompt": prompt}).collect()[0].COMPLETION
        return res

    @property
    def _identifying_params(self) -> Mapping[str, Any]:
        """Get the identifying parameters."""
        return {
            "session": self.session,
            "model": self.model,
        }
