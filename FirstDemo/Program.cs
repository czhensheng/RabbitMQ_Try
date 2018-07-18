using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace FirstDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            Send("queue", "czs11");
            Send("queue", "czs22");
            Send("queue", "czs33");
            Send("queue", "czs44");
            Send("queue", "czs55");
            Send("queue", "czs66");
            Send("queue", "czs77");
            Send("queue", "czs88");
            Receive("queue");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="hostName"></param>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public static IConnection GetConnection(string hostName = "localhost", int port = 5672, string userName = ConnectionFactory.DefaultUser, string password = ConnectionFactory.DefaultPass)
        {
            //创建连接链接到RabbitMQ服务器
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = hostName;
            connectionFactory.Port = port;
            connectionFactory.UserName = userName;
            connectionFactory.Password = password;
            //连接连接到broker
            return connectionFactory.CreateConnection();
        }

        public static void Send(string queue, string data)
        {
            using (IConnection connection = GetConnection())
            {
                //创建一个消息通道,在客户端的每个连接里，可建立多个channel，每个channel代表一个会话任务。类似与Hibernate中的Session
                //AMQP协议规定只有通过channel才能指定AMQP命令，所以仅仅在创建了connection后客户端还是不能发送消息的,必须要创建一个channel才行
                //RabbitMQ建议客户端线程之间不要共用Channel,至少要保证共用Channel的线程发送消息必须是串行的，但是建议尽量共用Connection
                //创建一个channel 使用它来发送AMQP指令
                using (IModel channel = connection.CreateModel())
                {
                    channel.QueueDeclare(
                        queue: queue,     //声明队列名
                        durable: false,    //是否持久化,为true的话，即使服务崩溃也不会丢失队列,, 队列的声明默认是存放到内存中的，如果rabbitmq重启会丢失，如果想重启之后还存在就要使队列持久化，保存到Erlang自带的Mnesia数据库中，当rabbitmq重启之后会读取该数据库（但是RabbitMQ Server停掉了，那么这些消息还是会丢失）
                        exclusive: false,  //是否排外,排外为true的队列只可以在本次的连接中被访问
                        autoDelete: false, //是否自动删除(在最后一个connection断开(consumers = 0)的时候)
                        arguments: null     //
                        );
                    channel.BasicPublish(
                        exchange: "",   //交换机名称，为空表示默认的或者没有命名的交换
                        routingKey: queue,  //路由名    
                        basicProperties: null,
                        body: Encoding.UTF8.GetBytes(data));
                    Console.WriteLine("send:" + queue);
                }
            }
        }

        public static void Receive(string queue)
        {
            using (IConnection connection = GetConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.QueueDeclare(
                        queue: queue,
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    //创建事件驱动的消费者类型，尽量不要使用while(ture)循环来获取消息
                    EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
      
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine("received:" + message);
                        Thread.Sleep(new Random().Next(1000, 1000 * 1));
                        Console.WriteLine("handled!");
                       channel.BasicAck(ea.DeliveryTag,false); //这个很重要，如果 autoAck=false时候，没有发确认回去，则待处理的消息就会被重新分发,但是RabitMQ会消耗越来越多的内存,因为这些没有被应答的消息不能够被释放
                    };
                    //autoAck=false 不自动消息确认 这时需要手动调用   channel.BasicAck(); 进行消息确认(即使正在处理消息的工作者被停掉,这些消息也不会丢失,所有没有被应答的消息会被重新发送给其他工作者)
                    //autoAck=true 自动消息确认，当消息被RabbitMQ发送给消费者（consumers）之后，马上就会在内存中移除
                    channel.BasicConsume(queue: queue, autoAck: false, consumer: consumer);
                    Console.WriteLine("press enter to exit");
                    Console.ReadLine();
                }
            }
        }
    }
}
