# Flow

The central component is the manager. It has to accomplish to following tasks
- register time provider
- unregister time provider
- list of current events
- keep track of time providers (are they still alive)


## Register time provider
Time provider generate some (unique) ID which is used for registration and to setup comm channels.
Time provider sends _RegisterProvider_ request to manager endpoint `racelog.register_provider`. This is mainly used for the directory listing and setup some data processors. 

The time provider will send all of its data to the topic `racelog.state.{id}`. The manager sets up the following processes which will all listen to this topic.


|Process|Purpose|
|--|--|
|live timing processor | provide live standings|
|archiver | archive the incoming messages for later replay purposes|
|analysis | provide some analysis data|


## Unregister time provider

This can happen due to following events
- time provider sends Unregister
- time provider hasn't sent data for X minutes

If any of these events are detected the manager sends a **QUIT** message to each process it has created for that provider. 



## Communication between manager and process

When a manager creates a client process the parameter are passed via Queues. The _id_ of the time provider is part of that queue message.
The client has to subscribe to the topic `manager.command.{id}`. This is the topic which is used by the manager to issue commands.

Such commands are
- QUIT


### Handle duplicate registration

TBD


# Manager 

## Register provider
Endpoint: `racelog.register_provider`

The manager expects a single argument. This argument is a dictionary of
```
id: the event id
manifests: {
    car: car manifest
    session: session manifest
    pit: pit manifest
    message: message manifest
}
```
The manifest describes the message format that is uses for the respective topic. When data is sent via a topic only the values are sent via the topic in an array. The manifest has to be used in order to get the meaning of the values.

## Unregister
Endpoint: `racelog.unregister_provider`

```
id: the event id

```

## Publish new provider
In order to inform the sub modules about a new time provider session the manager will publish the data from the registration to this topic. Interested modules may do their setup when recieving this message.

Topic: `racelog.manager.provider`

# Messages
All messages sent via WAMP topics follow this schema

```
type: MessageType
timestamp: number (unix seconds as float)
payload: data according to MessageType
```



