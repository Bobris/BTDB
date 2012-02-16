using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Service;

namespace SimpleTester
{
    public class ComplexServiceTest
    {
        IServer _server;
        IService _serverService;
        TcpipClient _client;
        IService _clientService;
        Func<Task> _clientFunc;

        Task ServerAction()
        {
            Console.WriteLine("ServerAction started");
            return Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(1000);
                    Console.WriteLine("ServerAction stopped");
                });
        }

        public void Run()
        {
            _server = new TcpipServer(new IPEndPoint(IPAddress.Any, 12345));
            _server.NewClient = ch =>
                {
                    _serverService = new Service(ch);
                    _serverService.RegisterLocalService<Func<Task>>(ServerAction);
                };
            _server.StartListening();

            _client = new TcpipClient(new IPEndPoint(IPAddress.Loopback, 12345));
            _clientService = new Service(_client);
            _clientService.OnNewRemoteService.Subscribe(s =>
                {
                    Console.WriteLine("NewRemoteService {0}", s);
                    _clientFunc = _clientService.QueryRemoteService<Func<Task>>();
                });
            _client.ConnectAsync();
            while (_clientFunc==null)
            {
                Thread.Sleep(1);
            }
            _clientFunc().Wait();
            _serverService.Dispose();
            try
            {
                _clientFunc().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            _server.StopListening();
        }

    }
}
