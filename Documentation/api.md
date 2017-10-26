**This is the documentation for neton**



# neton API

## Running a Single Machine Cluster

Let's start neton:

```sh
./start.sh
```

### Setting the value of a key

Let's set the first key-value pair in the datastore.
In this case the key is `/message` and the value is `Hello world`.

```sh
curl http://127.0.0.1:2110/keys/message -XPUT -d value="Hello world"
```

```json
{
    "action": "set",
    "node": {
        "dir"："false",
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


### Get the value of a key

We can get the value that we just set in `/message` by issuing a `GET` request:

```sh
curl http://127.0.0.1:2110/keys/message
```

```json
{
    "action": "get",
    "node": {
        "dir"："false",
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
    "node": {
        "dir"："false",
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
    "node": {
        "dir"："false",
        "key": "/foo",
        "value": "bar"
    }
}
```


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
    "node": {
        "dir"："false",
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
    "node": {
        "key": "/",
        "dir": "true",
        "nodes": [
            {
                "key": "/foo_dir",
                "dir": "true"
            },
            {
                "dir"："false",
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
    "node": {
        "key": "/",
        "dir": "true",
        "nodes": [
            {
                "key": "/foo_dir",
                "dir": "true",
                "nodes": [
                    {
                        "dir"："false",
                        "key": "/foo_dir/foo",
                        "value": "bar",
                    }
                ]
            },
            {
                "dir"："false",
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
    "node": {
        "dir": "true",
        "key": "/dir",
    }
}
```
