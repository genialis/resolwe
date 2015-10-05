"""Workflow Executors"""


class BaseFlowExecutor(object):

    """Represents a workflow executor"""

    def run(self, data_id, script):
        """Execute the script and save results."""
        raise NotImplementedError('subclasses of BaseFlowExecutor may require a run() method')

    def terminate(self, data_id):
        """Terminate a running script."""
        raise NotImplementedError('subclasses of BaseFlowExecutor may require a terminate() method')
