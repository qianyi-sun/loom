from omnigibson.envs import EnvironmentWrapper, Environment
from omnigibson.utils.ui_utils import create_module_logger
from omnigibson.learning.utils.eval_utils import ROBOT_CAMERA_NAMES

logger = create_module_logger("RGBWrapper")


class RGBWrapper(EnvironmentWrapper):
    """
    RGB-only wrapper that keeps default camera resolution and avoids dynamic
    sensor modality / resolution mutation that can destabilize syntheticdata.
    """

    def __init__(self, env: Environment):
        super().__init__(env=env)
        robot = env.robots[0]
        for camera_id, camera_name in ROBOT_CAMERA_NAMES["R1Pro"].items():
            if camera_id == "head":
                sensor_name = camera_name.split("::")[1]
                # Match training/inference camera aperture without touching resolution.
                robot.sensors[sensor_name].horizontal_aperture = 40.0
        env.load_observation_space()
        logger.info("Reloaded observation space (RGBWrapper).")
