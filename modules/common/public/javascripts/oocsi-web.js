var OOCSI = (function() {

  var wsUri = "ws://localhost/ws";
  var username;
  var handlers = {};
  var responders = {};
  var calls = {};
  var websocket;
  var logger = internalLog;
  var error = internalError;

  function init() {
    logger("CONNECTING to "  + wsUri);
    websocket = new WebSocket(wsUri);
    websocket.onopen = onOpen;
    websocket.onclose = onClose;
    websocket.onmessage = onMessage;
    websocket.onerror = onError;
  }

  function onOpen(evt) {
    if(websocket.readyState === WebSocket.OPEN) {
      submit(username);
    }  
    logger("CONNECTED");
  }

  function onClose(evt) {
    logger("DISCONNECTED");
  }

  function onMessage(evt) {
    if(evt.data !== 'ping') {
      try {
        var e = JSON.parse(evt.data);
        if(e.data.hasOwnProperty('_MESSAGE_ID') && calls.hasOwnProperty(e.data['_MESSAGE_ID'])) {
          var c = calls[e.data['_MESSAGE_ID']];

          if((+new Date) < c.expiration) {
            delete e.data['_MESSAGE_ID'];
            c.fn(e.data);
          }

          delete calls[e.data['_MESSAGE_ID']];
        } else if(handlers[e.recipient] !== undefined) {
          handlers[e.recipient].forEach((fn) => {fn(e)});
        } else {
          logger('no handler for event: ' + evt.data);
        }
      } catch(e) {
        logger('ERROR: parse exception for event data ' + evt.data);
      }
      logger('RESPONSE: ' + evt.data);
    } else if(evt.data.length > 0) {
      websocket.send(".");
    }
  }

  function onError(evt) {
    error();
    logger('ERROR: ' + evt);
  }

  function waitForSocket(fn) {
    if(!websocket || websocket.readyState === WebSocket.CONNECTING) {
      setTimeout(function() { waitForSocket(fn) }, 200);
    } else {
      fn();
    }
  } 

  function submit(message) {
    if(websocket && websocket.send(message)) {
      logger("SENT: " + message); 
    }
  }

  function internalClose() {
    websocket && websocket.close();
  }

  function internalLog(message) {
    // do nothing by default
  }

  function internalError() {
    // do nothing by default
  }

  function internalConnected() {
    return websocket.readyState === WebSocket.OPEN;
  }

  function internalSend(client, data) {
    internalConnected() && submit('sendjson ' + client + ' '+ JSON.stringify(data));
  } 

  function internalCall(call, data, timeout, fn) {
    if(internalConnected()) {
      var uuid = guid();
      calls[uuid] = {expiration: (+new Date) + timeout, fn: fn};
      data['_MESSAGE_ID'] = uuid;
      data['_MESSAGE_HANDLE'] = call;
      submit('sendjson ' + call + ' '+ JSON.stringify(data));
    }
  } 

  function internalRegister(call, fn) {
    if(internalConnected()) {
      responders[call] = {fn: fn};
      internalSubscribe(call, function(e) {
        var response = {'_MESSAGE_ID': e.data['_MESSAGE_ID']};
        fn(e.data, response);
        internalSend(e.sender, response);
      });
    }
  }

  function internalSubscribe(channel, fn) {
    if(internalConnected()) {
      submit('subscribe ' + channel);
      if(handlers[channel] === undefined) {
        handlers[channel] = [];
      }
      handlers[channel].push(fn);
    } 
  } 

  function internalUnsubscribe(channel) {
    if(internalConnected()) {
      submit('unsubscribe ' + channel);
      handlers[channel] = [];
    }
  }

  function internalReconnect() {
    if(!internalConnected() && websocket.readyState !== WebSocket.CONNECTING) {
      logger("RECONNECTING");
      init();
      // reconnect subscriptions
      waitForSocket(function() {
        for (ch in handlers) {
          submit('subscribe ' + ch);
        }
      });
    }
  }

  function guid() {
    function s4() {
      return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
    }
      return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
  }

  return {
    connect: function(server, clientName, fn) {
      wsUri = server;
      username = clientName && clientName.length > 0 ? clientName : "OOCSI-web/webclient_####";
      username = username.replace(/#/g, () => Math.floor(Math.random() * 10));
      handlers[username] = [fn];
      init();
      setInterval(internalReconnect, 1000);
    },
    send: function(recipient, data) {
      waitForSocket(function() {
        internalSend(recipient, data);
      });
    },
    call: function(call, data, timeout, fn) {
      waitForSocket(function() {
        internalCall(call, data, timeout, fn);
      });
    },
    register: function(call, fn) {
      waitForSocket(function() {
        internalRegister(call, fn);
      });
    },
    subscribe: function(channel, fn) {
      waitForSocket(function() {
        internalSubscribe(channel, fn);
      });
    },
    unsubscribe: function(channel) {
      waitForSocket(function() {
        internalUnsubscribe(channel);
      });
    },
    variable: function(channel, name, throttle = 500) {
      var listeners = [];
      var value;
      var lastUpdate = 0;

      function notify(newValue) {
        // notify all subscribers of a value update (even if value is same)
        listeners.forEach(function(listener) { listener(newValue); });
      }

      function accessor(newValue, broadcast = true) {
        // without arguments, we just return the current value
        if (arguments.length === 0) { return value; }

        // only update if timeout has passed
        if(lastUpdate + throttle > Date.now()) { return; }
        lastUpdate = Date.now();

        // remember whether changed, then update local value
        let changed = newValue !== value;
        value = newValue;

        // notify internal subscribers, regardless of change or not
        notify(value);

        // only update if new value is different
        if(changed && broadcast) {
          // send new value to OOCSI
          let data = {}
          data[name] = value
          internalSend(channel, data);
        }

        return value;
      }

      // allow subscribers to changes in the value
      accessor.subscribe = function(listener) { listeners.push(listener); };

      // subscribe to OOCSI for getting external updates on value
      this.subscribe(channel, function(e) { if(e.data.hasOwnProperty(name)) { accessor(e.data[name], false) } });

      return accessor;
    },
    isConnected: function() {
      return internalConnected();
    },
    handle: function() {
      return username;
    },
    close: function() {
      waitForSocket(function() {
        internalClose();
      });
    },
    handlers: function() {
      return handlers;
    },
    logger: function(fn) {
      logger = fn;
    },
    error: function (fn) {
      error = fn;
    },
    heyOOCSI: function(name) {
      var device_name = name || username;
      var data = {}
      data[device_name] = {}
      data[device_name]['properties'] = {
        device_id: username
      }
      data[device_name]['location'] = {}
      data[device_name]['components'] = {}
      var components = data[device_name]['components']
      logger(`Added heyOOCSI device ${device_name}`)

      return {
            add_property: function(property, propertyValue) {
           data[device_name]['properties'][property] = propertyValue;
        },
        add_location: function(location, latitude, longitude) {
           data[device_name]['properties'][location] = [latitude, longitude];
        },
        add_sensor_brick: function(sensor_name, sensor_channel, sensor_type, sensor_unit, sensor_default, icon) {
           components[sensor_name] = {}
           components[sensor_name]['channel_name'] = sensor_channel
           components[sensor_name]['type'] = 'sensor'
           components[sensor_name]['sensor_type'] = sensor_type
           components[sensor_name]['unit'] = sensor_unit
           components[sensor_name]['value'] = sensor_default
           components[sensor_name]['icon'] = icon
           logger(`Added ${sensor_name} to the components list for device ${device_name}.`)
        },
        add_number_brick: function(number_name, number_channel, number_min_max, number_unit, number_default, icon) {
           components[number_name] = {}
           components[number_name]['channel_name'] = number_channel
           components[number_name]['type'] = 'number'
           components[number_name]['min_max'] = number_min_max
           components[number_name]['unit'] = number_unit
           components[number_name]['value'] = number_default
           components[number_name]['icon'] = icon
           logger(`Added ${number_name} to the components list for device ${device_name}.`)
        },
        add_binary_sensor_brick: function(sensor_name, sensor_channel, sensor_type, sensor_default, icon) {
           components[sensor_name] = {}
           components[sensor_name]['channel_name'] = sensor_channel
           components[sensor_name]['type'] = 'binary_sensor'
           components[sensor_name]['sensor_type'] = sensor_type
           components[sensor_name]['state'] = sensor_default
           components[sensor_name]['icon'] = icon
           logger(`Added ${sensor_name} to the components list for device ${device_name}.`)
        },
        add_switch_brick: function(switch_name, switch_channel, switch_type, switch_default, icon) {
           components[switch_name] = {}
           components[switch_name]['channel_name'] = switch_channel
           components[switch_name]['type'] = 'switch'
           components[switch_name]['sensor_type'] = switch_type
           components[switch_name]['state'] = switch_default
           components[switch_name]['icon'] = icon
           logger(`Added ${switch_name} to the components list for device ${device_name}.`)
        },
        add_light_brick: function(light_name, light_channel, led_type, spectrum, mired_min_max, light_default_state, light_default_brightness, icon) {
           components[light_name] = {}
           components[light_name]['channel_name'] = light_channel
           components[light_name]['type'] = 'light'
           components[light_name]['ledType'] = led_type
           components[light_name]['spectrum'] = spectrum
           components[light_name]['min_max'] = mired_min_max
           components[light_name]['state'] = light_default_state
           components[light_name]['brightness'] = light_default_brightness
           components[light_name]['icon'] = icon
           logger(`Added ${light_name} to the components list for device ${device_name}.`)
        },
        submit: function() {
          OOCSI.send('heyOOCSI!', data)
        },
        sayHi: function() {
          OOCSI.send('heyOOCSI!', data)
        }
      };
    }
  };

})();
