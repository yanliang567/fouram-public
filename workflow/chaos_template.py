import copy

from deploy.commons.common_params import CLUSTER, STANDALONE, DeployArchitecture

from workflow.base import Base
from parameters.input_params import param_info, InputParamsBase
from commons.common_func import get_deploy_architecture
from commons.common_type import TeardownType, CommonCallable
from utils.util_log import log


class ChaosTemplate(Base):

    def chaos_template(self, input_params: InputParamsBase, deploy_mode=STANDALONE,
                       chaos_client_type=None, chaos_kind=None, chaos_watch_time=None,
                       default_chaos_config: CommonCallable = None, **kwargs):
        # pop self.deploy_delete from self.teardown_funcs
        self.teardown_funcs.pop(TeardownType.DeployDelete, None)

        log.info("[ChaosTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)
        kwargs.update({DeployArchitecture: get_deploy_architecture(
            param_info.deploy_architecture, kwargs.get(DeployArchitecture, None))})

        input_params.deploy_mode = input_params.deploy_mode or deploy_mode
        self.chaos_default(
            deploy_tool=input_params.deploy_tool, deploy_mode=input_params.deploy_mode,
            chaos_client_type=chaos_client_type, chaos_kind=chaos_kind, chaos_watch_time=chaos_watch_time,
            chaos_config=input_params.chaos_config, default_chaos_config=default_chaos_config, **kwargs
        )
