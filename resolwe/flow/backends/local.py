from resolwe.flow.backends import BaseFlowBackend


class FlowBackend(BaseFlowBackend):

    def run(self, data_id, script):
        print('RUN: {} {}'.format(data_id, script))
