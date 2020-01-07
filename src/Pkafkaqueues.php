<?php
namespace ClevePHP\Drives\Queues\pkafka;
class Pkafkaqueues
{

    static $driveObject;

    static $config = [];

    public static function driveObject($configs)
    {
        self::$config = $configs;
        return (new self());
    }

    public static function push($data, $toppic = "list")
    {
        $conf = [
            "toppic" => $toppic,
            "group_id" => isset(self::$config['group_id']) ? self::$config['group_id'] : "T-001"
        ];
        if (self::$config) {
            $conf = array_merge($conf, self::$config);
        }
        if (is_array($data)) {
            $data = json_encode($data);
        }
        $config = (\ClevePHP\Drives\Queues\pkafka\Config::getInstance())->loadConfig($conf);
        return (\ClevePHP\Drives\Queues\pkafka\Producers::getInstance())->config($config)->produce($data);
    }

    public static function pop(callable $callback, $toppic = "list")
    {
        $conf = [
            "toppic" => $toppic,
            "group_id" => isset(self::$config['group_id']) ? self::$config['group_id'] : "T-001"
        ];
        if (self::$config) {
            $conf = array_merge($conf, self::$config);
        }
        $config = (\ClevePHP\Drives\Queues\pkafka\Config::getInstance())->loadConfig($conf);

        (\ClevePHP\Drives\Queues\pkafka\Consumer::getInstance())->config($config)->consumer(function ($result) use ($callback) {
           
            if ($result) {
                if ($result && isset($result["message"])) {
                    ($callback instanceof \Closure) && call_user_func($callback, new class($result){
                        public $payload;
                        public $offset;
                        function __construct($resutl) {
                            $jsonData=!empty($resutl["message"]["value"])?@json_decode($resutl["message"]["value"], TRUE):null;
                            $this->payload = $jsonData??$resutl["message"]["value"];
                            $this->offset =$resutl["offset"]??0;
                        }
                    });
                    
                }
            }
            
        });
    }
}
