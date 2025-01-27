import rclpy
from rclpy.node import Node
from sensor_msgs.msg import JointState
from kafka_producer import KafkaProducer

KAFKA_BOOTSTRAP_SERVER = '192.168.3.99:9097,192.168.3.99:9098,192.168.3.99:9099'
KAFKA_API_KEY = 'theengineroom'
KAFKA_API_SECRET = "1tY=ZP43t20"

KAFKA_TOPIC = "robot_0_joint_trajectory"

class MinimalSubscriber(Node):

    def __init__(self):
        super().__init__('subscriber')
        self.kp = KafkaProducer(KAFKA_BOOTSTRAP_SERVER, KAFKA_API_KEY, KAFKA_API_SECRET)
        self.subscription = self.create_subscription(
            JointState,
            '/joint_states',
            self.listener_callback,
            10)
        self.subscription  # prevent unused variable warning

    def listener_callback(self, msg):
        msg.effort=[0.0,0.0,0.0,0.0,0.0,0.0]
        self.kp.produce_record(KAFKA_TOPIC, msg)

def main(args=None):
    rclpy.init(args=args)

    minimal_subscriber = MinimalSubscriber()

    rclpy.spin(minimal_subscriber)

    minimal_subscriber.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
