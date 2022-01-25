from abc import abstractmethod
from logging import Logger, getLogger

from attrs import define


@define
class BaseScript(object):
    data: dict
    context: dict
    logger: Logger = getLogger(__name__)
    code = ''
    name = ''
    @abstractmethod
    async def handle(self):
        """
            execute script main logic
        """
