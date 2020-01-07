<?php
namespace ClevePHP\Drives\Queues\pkafka;

date_default_timezone_set('PRC');
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;

class Producers
{

    protected $producer;

    protected $topic;

    protected $topicConf;

    protected $config;

    private static $instance;

    private function __construct()
    {}

    private function __clone()
    {}

    static public function getInstance()
    {
        if (! self::$instance instanceof self) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public function config(\ClevePHP\Drives\Queues\pkafka\Config $config)
    {
        $this->config = $config;
        $this->topic = $config->toppic;
        unset($config);
        return $this;
    }

    public function produce($message)
    {
        $logger = new Logger($this->config->toppic);
        // Now add some handlers
        $logger->pushHandler(new StdoutHandler());
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs($this->config->messageTimeoutMs);
        $config->setMetadataBrokerList($this->config->metadataBrokerList);
        $config->setBrokerVersion($this->config->brokerVersion);
        $config->setRequiredAck($this->config->requiredAck);
        $config->setIsAsyn($this->config->isAsyn);
        $config->setProduceInterval($this->config->produceInterval);

        $producer = new \Kafka\Producer(function () use ($message) {
            return array(
                array(
                    'topic' => $this->config->toppic,
                    'value' => $message,
                    'key' => $this->config->toppic
                )
            );
        });
        if ($this->config->debugLogLevel) {
            $producer->setLogger($logger);
        }

        $producer->success(function ($result) {
            return true;
        });
        $producer->error(function ($errorCode) {
            throw new \Exception($errorCode);
        });
        $producer->send(true);
    }

    protected function getConfig(): ?\ClevePHP\Drives\Queues\pkafka\Config
    {
        return $this->config;
    }
}