# Services are used to automatically run the Data Foundry on a server

## H2 database server

Look at the local `h2.service` file.

Create it in:

````
/etc/systemd/system/h2.service
````

Start it for a first test:

````
sudo systemctl start h2.service
````

Stop it for a first test:

````
sudo systemctl stop h2.service
````

Install it:

````
sudo systemctl enable h2.service
````

## Nginx

### Logging settings

````
	##
    # Logging Settings
    ##
    log_format timed_combined '$remote_addr - $remote_user [$time_local] ' '"$request" $status $body_bytes_sent ' '"$http_referer" "$http_user_agent" ' '$request_time $upstream_response_time $pipe';

    access_log /var/log/nginx/access.log timed_combined;
    error_log /var/log/nginx/error.log;

````

### Upstream settings

````
 upstream foundry-backend {
      zone upstreams 64K;
      server 127.0.0.1:8888;

      keepalive 32;
}

````


## Foundry service

See `foundry.service`

How to check the output of a service: 

````
journalctl -u h2
````

````
journalctl -u foundry
````



