<?php
namespace ClevePHP\Drives\Queues\pkafka;

class Pkafkaconnectionpool
{

    private static $instance;

    private $connectName;

    private $connectObjects = [];

    private function __construct()
    {}

    private  function producers()
    {
        $this->connectName = "producers";
        return $this;
    }

    private function consumer()
    {
        $this->connectName = "consumer";
        return $this;
    }
    private  function connect($config)
    {
       try {
           if (empty($config)) {
               throw new \Exception("kafka config is null");
           }
           $this->config = $config;
           $connectionNum = 2;
           if (isset($config["connection_num"])) {
               $connectionNum = $config["connection_num"];
           }
           if (empty($this->tag)) {
               $this->tag = "devault";
           }
           $config = (\ClevePHP\Drives\Queues\pkafka\Config::getInstance())->loadConfig($this->config);
           for ($i = 0; $i < $connectionNum; $i ++) {
               $this->connectObjects["producers"][$this->tag][] = (\ClevePHP\Drives\Queues\pkafka\Producers::getInstance())->config($config);
               $this->connectObjects["consumer"][$this->tag][] = (\ClevePHP\Drives\Queues\pkafka\Consumer::getInstance())->config($config);
               
           }
           return $this;
       } catch (\Exception $e) {
           print_r($e->__toString());
       }
    }

    public function setTag($tag)
    {
        $this->tag = $tag;
        return $this;
    }

    private function __clone()
    {
        // TODO: Implement __clone() method.
    }

    public static function getInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public function getConfig()
    {
        return $this->config;
    }
    // 获取一条链接
    private function getConnect()
    {
        try {
            if (empty($this->connectName)) {
                throw new \Exception($this->connectName . " is null");
            }
            $avilConnectionNum = count($this->connectObjects[$this->connectName][$this->tag]);
            if ($avilConnectionNum == 0) {
                return false;
            }
            $kafka = null;
            $kafka = array_pop($this->connectObjects[$this->connectName][$this->tag]);
            array_push($this->connectObjects[$this->connectName][$this->tag], $kafka);
            return $kafka;
        } catch (\Throwable $e) {
            print_r($e->__toString());
        }
    }
    public function getProducers($config):?\ClevePHP\Drives\Queues\pkafka\Producers {
        if ($config) {
            $result=$this->connect($config)->producers()->getConnect();
            if(!$result){
                $this->connect($config)->producers()->connect($config);
            }
            return $this->connect($config)->producers()->getConnect();
        }
        return false;
    }
    public function getConsumer($config):?\ClevePHP\Drives\Queues\pkafka\Consumer  {
        if ($config) {
            $result=$this->connect($config)->consumer()->getConnect();
            if(!$result){
                $this->connect($config)->consumer()->connect($config);
            }
            return $this->connect($config)->consumer()->getConnect();
        }
        return false;
    }
    
}