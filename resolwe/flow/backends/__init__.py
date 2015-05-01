"""Workflow backends"""


class BaseFlowBackend(object):

    """Represents a workflow backend"""

    def run(self, data_id, script):
        """Execute the script and save results."""
        raise NotImplementedError('subclasses of BaseFlowBackend may require a run() method')

    def terminate(self, data_id):
        """Terminate a running script."""
        raise NotImplementedError('subclasses of BaseFlowBackend may require a terminate() method')
