class BaseFlowBackend(object):

    def run(self, data_id, script):
        """Execute the script and save results."""
        raise NotImplementedError('subclasses of BaseFlowBackend may require a run() method')
