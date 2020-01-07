<?php
namespace ClevePHP\Drives\Queues\pkafka;
date_default_timezone_set('PRC');
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;
class Consumer
{

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

    protected $producer;

    protected $topic;

    protected $config;

    public function config(\ClevePHP\Drives\Queues\pkafka\Config $config)
    {
        $this->config = $config;
        $this->topic = $config->toppic;
        unset($config);
        return $this;
    }
    public function consumer(callable $callback)
    {
        
        $logger = new Logger($this->config->toppic);
        // Now add some handlers
        $logger->pushHandler(new StdoutHandler());
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs($this->config->produceInterval);
        $config->setMetadataBrokerList($this->config->metadataBrokerList);
        $config->setGroupId($this->config->gropuId);
        $config->setBrokerVersion($this->config->metadataBrokerList);
        $config->setTopics(array($this->config->toppic));
        $config->setOffsetReset($this->config->autoOffsetReset);
        $consumer = new \Kafka\Consumer();
        if ($this->config->debugLogLevel) {
            $consumer->setLogger($logger);
        }
        $consumer->start(function($topic, $part, $message) use($callback) {
            ($callback instanceof \Closure) && call_user_func($callback, $message);
        });
        
    }
    protected function getConfig(): ?\ClevePHP\Drives\Queues\pkafka\Config
    {
        return $this->config;
    }
}

