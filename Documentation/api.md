**This is the documentation for neton**



# neton API

## Running a Single Machine Cluster

Let's start neton:

```sh
./start.sh
```

## KV Store 

### Setting the value of a key

Let's set the first key-value pair in the datastore.
In this case the key is `/message` and the value is `Hello world`.

```sh
curl http://127.0.0.1:2110/keys/message -XPUT -d value="Hello world"
```

```json
{
    "action": "set",
    "netonIndex":1,
    "node": {
        "dir":"false",
        "key": "/message",
        "value": "Hello world"
    }
}
```

The response object contains several attributes:

1. `action`: the action of the request that was just made.
The request attempted to modify `node.value` via a `PUT` HTTP request, thus the value of action is `set`.

2. `node.key`: the HTTP path to which the request was made.
We set `/message` to `Hello world`, so the key field is `/message`.
neton uses a file-system-like structure to represent the key-value pairs, therefore all keys start with `/`.

3. `node.value`: the value of the key after resolving the request.
In this case, a successful request was made that attempted to change the node's value to `Hello world`.

4. `node.netonIndex`: an index is a unique, monotonically-incrementing integer created for each change to neton.
This specific index reflects the point in the neton state member at which a given key was created.


### Get the value of a key

We can get the value that we just set in `/message` by issuing a `GET` request:

```sh
curl http://127.0.0.1:2110/keys/message
```

```json
{
    "action": "get",
    "netonIndex":1,
    "node": {
        "dir":"false",
        "key": "/message",
        "value": "Hello world"
    }
}
```


### Changing the value of a key

You can change the value of `/message` from `Hello world` to `Hello neton` with another `PUT` request to the key:

```sh
curl http://127.0.0.1:2110/keys/message -XPUT -d value="Hello neton"
```

```json
{
    "action": "set",
    "netonIndex":2,
    "node": {
        "dir":"false",
        "key": "/message",
        "value": "Hello neton"
    }
}
```

### Deleting a key

You can remove the `/message` key with a `DELETE` request:

```sh
curl http://127.0.0.1:2110/keys/message -XDELETE
```

```json
{
    "action": "delete",
    "netonIndex":1,
    "node": {
        "key": "/message",
    }
}
```


### Waiting for a change

We can watch for a change on a key and receive a notification by using long polling.
This also works for child keys by passing `recursive=true` in curl.

In one terminal, we send a `GET` with `wait=true` :

```sh
curl http://127.0.0.1:2110/keys/foo?wait=true
```

Now we are waiting for any changes at path `/foo`.

In another terminal, we set a key `/foo` with value `bar`:

```sh
curl http://127.0.0.1:2110/keys/foo -XPUT -d value=bar
```

The first terminal should get the notification and return with the same response as the set request:

```json
{
    "action": "set",
    "netonIdex":7,
    "node": {
        "dir":"false",
        "key": "/foo",
        "value": "bar"
    }
}
```

However, the watch command can do more than this.
Using the index, we can watch for commands that have happened in the past.
This is useful for ensuring you don't miss events between watch commands.
Typically, we watch again from the `netonIndex` + 1 of the node we got.

Let's try to watch for the set command of index 7 again:

```sh
curl 'http://127.0.0.1:2110/keys/foo?wait=true&waitIndex=7'
```

The watch command returns immediately with the same response as previously.

If we were to restart the watch from index 8 with:

```sh
curl 'http://127.0.0.1:2110/keys/foo?wait=true&waitIndex=8'
```

Then even if neton is on index 9 or 800, the first event to occur to the `/foo`
key between 8 and the current index will be returned.

**Note**: neton only keeps the responses of the most recent 1000 events across all neton keys.
It is recommended to send the response to another thread to process immediately
instead of blocking the watch while processing the result.

### Creating Directories

In most cases, directories for a key are automatically created.
But there are cases where you will want to create a directory or remove one.

Creating a directory is just like a key except you cannot provide a value and must add the `dir=true` parameter.

```sh
curl http://127.0.0.1:2110/keys/dir -XPUT -d dir=true
```
```json
{
    "action": "create",
    "netonIndex":1,
    "node": {
        "dir": true,
        "key": "/dir",
    }
}
```

### Listing a directory

In neton we can store two types of things: keys and directories.
Keys store a single string value.
Directories store a set of keys and/or other directories.

In this example, let's first create some keys:

We already have `/foo=two` so now we'll create another one called `/foo_dir/foo` with the value of `bar`:

```sh
curl http://127.0.0.1:2110/keys/foo_dir/foo -XPUT -d value=bar
```

```json
{
    "action": "set",
    "netonIndex":2,
    "node": {
        "dir":"false",
        "key": "/foo_dir/foo",
        "value": "bar"
    }
}
```

Now we can list the keys under root `/`:

```sh
curl http://127.0.0.1:2110/keys
```

We should see the response as an array of items:

```json
{
    "action": "get",
    "netonIndex":2,
    "node": {
        "key": "/",
        "dir": "true",
        "nodes": [
            {
                "key": "/foo_dir",
                "dir": "true"
            },
            {
                "dir":"false",
                "key": "/foo",
                "value": "two",
            }
        ]
    }
}
```

Here we can see `/foo` is a key-value pair under `/` and `/foo_dir` is a directory.
We can also recursively get all the contents under a directory by adding `recursive=true`.

```sh
curl http://127.0.0.1:2110/keys/?recursive=true
```

```json
{
    "action": "get",
    "netonIndex":2,
    "node": {
        "key": "/",
        "dir": "true",
        "nodes": [
            {
                "key": "/foo_dir",
                "dir": "true",
                "nodes": [
                    {
                        "dir":"false",
                        "key": "/foo_dir/foo",
                        "value": "bar",
                    }
                ]
            },
            {
                "dir":"false",
                "key": "/foo",
                "value": "two",
            }
        ]
    }
}
```


### Deleting a Directory


To delete a directory that holds keys, you must add `recursive=true`.

```sh
curl http://127.0.0.1:2110/keys/dir?recursive=true -XDELETE
```

```json
{
    "action": "delete",
    "netonIndex":2,
    "node": {
        "dir": "true",
        "key": "/dir",
    }
}
```

## Services

Neton allows services to register their instance information and to discover providers of a given service.

### Register

```sh
curl http://127.0.0.1:2110/register -XPOST --data '{
"service": {
    "id": "redis1",
    "name": "redis",
    "address": "127.0.0.1",
    "port": 8000
  },
  "check": {
    "http": "localhost:8888",
    "interval": 10,
    "timeout" : 10
    }
}'
```

The register contains several attributes:

1. `id`: it's a unique ID for subnode.

2. `name`: service name.

3. `address`:  It's used to specify a service-specific IP address.

4. `port`: the port of service.

5. `check`: These checks make an HTTP `GET` request every Interval (e.g. every 30 seconds) to the specified URL. The status of the service depends on the HTTP response code:  200 code is considered passing and anything else is a failure. By default, HTTP checks will be configured with a request timeout equal to the check interval, with a max of 10 seconds. It is possible to configure a custom HTTP check timeout value by specifying the `timeout` field in the check definition. 

### Deregister

```sh
curl http://127.0.0.1:2110/deregister -XPOST --data '{
 "id" : "redis1",
 "name" : "redis"
}'
```

The Deregister contains several attributes:

1. `id`: The unique ID for subnode.

2. `name`: service name.

### Check Services

```sh
curl http://127.0.0.1:2110/keys/service/{name}/{id}
```
The response object like this :

```json
{
    "action": "get",
    "netonIndex": 0,
    "node": {
        "dir": "false",
        "key": "/service/redis/redis1",
        "value": {
            "check": {
                "http": "redis1.putao.com",
                "interval": 20,
                "timeout": 10
            },
            "service": {
                "address": "127.0.0.1",
                "id": "redis1",
                "name": "redis",
                "port": 8001
            },
            "status": "passing"
        }
    }
}
```

## Health

Neton support health checks.