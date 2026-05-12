from typing import Optional, Union, List, Dict, Any

from ..config import StepConfig, LocalisationStringType, LocalisationString, UserStepConfig, StepUserConfig, EventType
from ..configuration import ConfigurationManager, ConfigurationState


class GetConfiguration(StepConfig):
    def __init__(
        self,
        configuration_type: str,
        name: str = "getConfig",
        display_name: LocalisationStringType = LocalisationString("Get Configuration", "Get Configuration"),
        description: Optional[LocalisationStringType] = None,
        user_config_name: LocalisationStringType = LocalisationString("Get Configuration", "Get Configuration"),
    ):
        self._name = name
        self._description = description
        self._display_name = display_name
        self._user_config_name = user_config_name
        self._configuration_type = configuration_type

    async def run(self, user_config: Optional[UserStepConfig], results, **_):
        if user_config is None:
            raise FileNotFoundError("User config not provided")
        user_input = user_config.get("getTechnologyConfiguration")
        if user_input is None:
            raise FileNotFoundError("User config not found")
        configuration_id = user_input.get("configurationId")
        version_id = user_input.get("versionId")
        if configuration_id is None:
            raise FileNotFoundError("Configuration not provided")
        manager = ConfigurationManager([])
        if version_id is None:
            yield f'Get latest active version for configuration "{configuration_id}" of type "{self._configuration_type}"', EventType.INFO
            version = manager.get_latest_version(configuration_id, [ConfigurationState.ACTIVE])
        else:
            yield f'Get version "{version_id}" for configuration "{configuration_id}" of type "{self._configuration_type}"', EventType.INFO
            version = manager.get_version(configuration_id, version_id)
        yield version.configuration, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                self.name(),
                LocalisationString("Technology Configuration", "Technologie Konfiguration"),
                None,
                StepUserConfig.StepUserConfigType.CONFIGURATION,
                configurationType=self._configuration_type,
            )
        ]

    def name(self) -> str:
        return self._name

    def display_name(self) -> LocalisationStringType:
        return self._display_name

    def description(self) -> LocalisationStringType:
        return self._description

    def dependencies(self) -> Union[List[str], None]:
        return None
