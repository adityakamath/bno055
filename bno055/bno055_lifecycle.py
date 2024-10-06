import sys
import threading
from typing import Optional
from bno055.connectors.i2c import I2C
from bno055.connectors.uart import UART
from bno055.error_handling.exceptions import BusOverRunException
from bno055.params.NodeParameters import NodeParameters
from bno055.sensor.SensorService import SensorService
import rclpy
from rclpy.lifecycle import Node, State, TransitionCallbackReturn
from rclpy.timer import Timer
from rclpy.executors import ExternalShutdownException

class Bno055LifecycleNode(Node):
    """
    ROS2 Lifecycle Node for interfacing Bosch Bno055 IMU sensor.

    :param Node: ROS2 Lifecycle Node Class to initialize from
    :type Node: ROS2 Lifecycle Node
    :raises NotImplementedError: Indicates feature/function is not implemented yet.
    """
    
    def __init__(self):
        super().__init__('bno055')
        
        self.sensor = None
        self.param = NodeParameters(self)
        self.lock = threading.Lock()
        self.data_query_timer: Optional[Timer] = None
        self.status_timer: Optional[Timer] = None

    def on_configure(self, state: State) -> TransitionCallbackReturn:
        """Configure the node and parameters."""
        try:
            if self.param.connection_type.value == UART.CONNECTIONTYPE_UART:
                connector = UART(self,
                                 self.param.uart_baudrate.value,
                                 self.param.uart_port.value,
                                 self.param.uart_timeout.value)
            elif self.param.connection_type.value == I2C.CONNECTIONTYPE_I2C:
                connector = I2C(self,
                                self.param.i2c_bus.value,
                                self.param.i2c_addr.value)
            else:
                raise NotImplementedError('Unsupported connection type: '
                                          + str(self.param.connection_type.value))

            connector.connect()
            self.sensor = SensorService(self, connector, self.param)
            self.sensor.configure()

            self.get_logger().info("Configured")
            return TransitionCallbackReturn.SUCCESS
        except Exception as e:
            self.get_logger().error(f"Configuration failed: {e}")
            return TransitionCallbackReturn.FAILURE

    def on_activate(self, state: State) -> TransitionCallbackReturn:
        """Activate the node (start timers and data acquisition)."""
        try:
            # Start data query timer
            f = 1.0 / float(self.param.data_query_frequency.value)
            self.data_query_timer = self.create_timer(f, self.read_data)

            # Start calibration status timer
            f = 1.0 / float(self.param.calib_status_frequency.value)
            self.status_timer = self.create_timer(f, self.log_calibration_status)

            self.get_logger().info("Activated")
        except Exception as e:
            self.get_logger().error(f"Activation failed: {e}")

        return super().on_activate(state)

    def on_deactivate(self, state: State) -> TransitionCallbackReturn:
        """Deactivate the node (stop timers and publishing)."""
        try:
            if self.data_query_timer is not None:
                self.destroy_timer(self.data_query_timer)
            if self.status_timer is not None:
                self.destroy_timer(self.status_timer)

            self.get_logger().info("Deactivated")
        except Exception as e:
            self.get_logger().error(f"Deactivation failed: {e}")
        
        return super().on_deactivate(state)

    def on_cleanup(self, state: State) -> TransitionCallbackReturn:
        """Clean up node resources."""
        self.get_logger().info("Cleaning up...")
        return TransitionCallbackReturn.SUCCESS

    def on_shutdown(self, state: State) -> TransitionCallbackReturn:
        """Shutdown the node."""
        self.get_logger().info("Shutting down...")
        return TransitionCallbackReturn.SUCCESS

    def read_data(self):
        """Periodic data_query_timer executions to retrieve sensor IMU data."""
        if self.lock.locked():
            self.get_logger().warn('Message communication in progress - skipping query cycle')
            return

        self.lock.acquire()
        try:
            self.sensor.get_sensor_data()
        except BusOverRunException:
            return
        except ZeroDivisionError:
            return
        except Exception as e:
            self.get_logger().warn(f'Receiving sensor data failed with {type(e).__name__}: {e}')
        finally:
            self.lock.release()

    def log_calibration_status(self):
        """Periodic logging of calibration data (quality indicators)."""
        if self.lock.locked():
            self.get_logger().warn('Message communication in progress - skipping query cycle')
            return

        self.lock.acquire()
        try:
            self.sensor.get_calib_status()
        except Exception as e:
            self.get_logger().warn(f'Receiving calibration status failed with {type(e).__name__}: {e}')
        finally:
            self.lock.release()


def main(args=None):
    """Main entry method for this ROS2 node."""
    rclpy.init()
    node = Bno055LifecycleNode()
    
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    except ExternalShutdownException:
        sys.exit(1)
    finally:
        try:
            if node.data_query_timer is not None:
                node.destroy_timer(node.data_query_timer)
            if node.status_timer is not None:
                node.destroy_timer(node.status_timer)
        except UnboundLocalError:
            node.get_logger().info('No timers to shutdown')
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()