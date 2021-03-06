# Client Controller

The client controller is the entry point for making changes to the Inspr tree structure. By using the client, it is possible to create, modify and delete dApps, Channels, Types and Aliases, as well as get authorization to access Insprd and see its available brokers.

## Instantiating a new Client

The structure of the `client` basically needs a `ControllerConfig` that is used to define the client's `request.Client` configuration, such as where the requests will be sent to, it's headers and authentication token. So the first step is to instantiate a new `ControllerConfig`:

```go
config := client.ControllerConfig{
    Auth: Authenticator{
        tokenPath,
    },
    URL: url,
    Scope: scope,
}
```
Where `url` is the route to which Inspr Daemon is listening, `tokenPath` is the path to the file that contains the authentication token and `scope` is the path to the desired Inspr structure where the requests will be made to (if empty, the default scope will be the root dApp).

Then, use the `NewControllerClient` function defined in the `client` package passing the created `ControllerConfig` as a parameter to instantiate a new client controller:

```go
client := client.NewControllerClient(config)
```

## Using the Client Controller

To use the client, call the respective function to the type of structure you want to manipulate followed by the operation function that must be done. For example, to create a new dApp `HelloWorldApp` inside the root dApp, just do:

```go
resp, err := client.Apps().Create(context.Background(), "", &meta.App{
    Meta: meta.Metadata{
        Name: "HelloWorldApp",
    },
    Spec: meta.AppSpec{},
}, dryRun)
```

In the example above, the function for creating a dApp receives:
*  A [go context](https://golang.org/pkg/context/) (context.Background())
*  The path in which that dApp will be created ("")
*  The dApp itself (&meta.App{...})
*  The dryRun flag, which is present in all methods other than `get`. It's a bool that indicates whether the modifications should really be applied to the structure or if they are simply used to visualize which changes would be made.

Similarly, to create a Channel called "NewChannel" within the `HelloWorldApp` dApp that was just created, do:

```go
resp, err := client.Channels().Create(context.Background(), "HelloWorldApp", &meta.Channel{
    Meta: meta.Metadata{
        Name: "NewChannel",
    },
    Spec: meta.ChannelSpec{
        Type: "TypeHello",
    },
}, dryRun)
```
Remember that, in the case above, the Type `TypeHello` must exist within `HelloWorldApp`.

## Apps

### func \(\*AppClient) Get

```go
func (ac *AppClient) Get(ctx context.Context, scope string) (*meta.App, error)
```
`Get` retrieves information of a dApp that exists in Insprd. The `scope string` refers to the dApp itself, represented with a dot separated query, such as **app1.app2**.  
So to get a dApp called `app2` that is inside of `app1`, you would call:
```go 
ac.Get(context.Background(), "app1.app2")
```

### func \(\*AppClient) Create

```go
func (ac *AppClient) Create(ctx context.Context, scope string, app *meta.App, dryRun bool) (diff.Changelog, error)
```
`Create` creates a dApp in Insprd. The `scope string` refers to the dApp where the actual dApp will be instantiated in, represented with a dot separated query, such as **app1.app2**. The information of the dApp, such as name and other specifications, will be extracted from the definition of the dApp itself.   
So to create a dApp `app2` inside of `app1` you would call:
```go
ac.Create(context.Background(), "app1", &meta.App{...}, false)
```

### func \(\*AppClient) Update

```go
func (ac *AppClient) Update(ctx context.Context, scope string, app *meta.App, dryRun bool) (diff.Changelog, error)
```
`Update` updates a dApp in Insprd. If the dApp doesn't exist, it will return a error. The `scope string` refers to the dApp where the actual dApp resides, represented with a dot separated query, such as **app1.app2**. The information of the dApp, such as name and other specifications, will be extracted from the definition of the dApp itself.   
So to update a dApp `app2` which is inside of `app1` you would call:
```go
ac.Update(context.Background(), "app1", &meta.App{...}, false)
```

### func \(\*AppClient) Delete

```go
func (ac *AppClient) Delete(ctx context.Context, scope string, dryRun bool) (diff.Changelog, error)
```
`Delete` deletes a dApp that exists in Insprd. The `scope string` refers to the dApp itself, represented with a dot separated query, such as **app1.app2**.  
So to delete a dApp `app2` that is inside `app1` you would call:
```go
ac.Delete(context.Background(), "app1.app2")
```

## Channels

### func \(\*ChannelClient) Get

```go
func (cc *ChannelClient) Get(ctx context.Context, scope, name string) (*meta.Channel, error)
```
`Get` retrieves information of a Channel that exists in Insprd. The `scope string` refers to the dApp that contains the Channel, represented with a dot separated query, such as **app1.app2**. The `name` paramenter is the name of the Channel.  
So to search for a Channel `channel1` that is inside a dApp `app1` you would call:
```go
cc.Get(context.Background(), "app1", "channel1")
```

### func \(\*ChannelClient) Create

```go
func (cc *ChannelClient) Create(ctx context.Context, scope string, ch *meta.Channel, dryRun bool) (diff.Changelog, error)
```
`Create` creates a Channel in Insprd. The `scope string` refers to the dApp that will contain the Channel, represented with a dot separated query, such as **app1.app2**. The Channel information such as its name and type will be extracted from the given Channel's definition.  
So to create a Channel `channel1` that is inside a dApp `app1` you would call:
```go
cc.Create(context.Background(), "app1", &meta.Channel{...})
```

### func \(\*ChannelClient) Update

```go
func (cc *ChannelClient) Update(ctx context.Context, scope string, ch *meta.Channel, dryRun bool) (diff.Changelog, error)
```
`Update` updates a Channel in Insprd. The `scope string` refers to the dApp that contains the Channel, represented with a dot separated query, such as **app1.app2**. The Channel information such as its name and type will be extracted from the given Channel's definition.  
So to update a Channel `channel1` that is inside a dApp `app1` you would call:
```go
cc.Update(context.Background(), "app1", &meta.Channel{...})
```

### func \(\*ChannelClient) Delete

```go
func (cc *ChannelClient) Delete(ctx context.Context, scope, name string, dryRun bool) (diff.Changelog, error)
```
`Delete` deletes a Channel that exists in Insprd. The `scope string` refers to the dApp that contains the Channel, represented with a dot separated query, such as **app1.app2**. The `name` paramenter is the name of the Channel to be deleted.  
So to delete a Channel `channel1` that is inside a dApp `app1` you would call:
```go
cc.Delete(context.Background(), "app1", "channel1")
```

## Types

### func \(\*TypeClient) Get

```go
func (tc *TypeClient) Get(ctx context.Context, scope, name string) (*meta.Type, error)
```
`Get` retrieves information of a Type in Insprd. The `scope string` refers to the dApp that contains the Type, represented with a dot separated query, such as **app1.app2**. The `name` parameter is the name of the Type.  
So to search for a Type `type1` that is inside of a dApp `app1` you would call:
```go
tc.Get(context.Background(), "app1", "type1")
```

### func \(\*TypeClient) Create

```go
func (tc *TypeClient) Create(ctx context.Context, scope string, t *meta.Type, dryRun bool) (diff.Changelog, error)
```
`Create` creates a Type in Insprd. The `scope string` refers to the dApp that will contain the Type, represented with a dot separated query, such as **app1.app2**. The Type information such as its name and schema will be extracted from the given Type's definition.  
So to create a Type `type1` inside of a dApp `app1` you would call:
```go
tc.Create(context.Background(), "app1", &meta.Type{...})
```

### func \(\*TypeClient) Update

```go
func (tc *TypeClient) Update(ctx context.Context, scope string, t *meta.Type, dryRun bool) (diff.Changelog, error)
```
`Update` updates a Type in Insprd. The `scope string` refers to the dApp that contains the Type, represented with a dot separated query, such as **app1.app2**. The Type information such as its name and schema will be extracted from the given Type's definition.  
So to update a Type `type1` that is inside of a dApp `app1` you would call:
```go
tc.Create(context.Background(), "app1", &meta.Type{...})
```

### func \(\*TypeClient) Delete

```go
func (tc *TypeClient) Delete(ctx context.Context, scope, name string, dryRun bool) (diff.Changelog, error)
```
`Delete` deletes a Type that exists in Insprd. The `scope string` refers to the dApp that contains the Type, represented with a dot separated query, such as **app1.app2**. The `name` parameter is the name of the Type to be deleted.   
So to delete a Type `type1` that is inside of a dApp `app1` you would call:
```go
tc.Delete(context.Background(), "app1", "type1")
```

## Alias

### func \(\*AliasClient) Get

```go
func (ac *AliasClient) Get(ctx context.Context, scope, key string) (*meta.Alias, error)
```
`Get` retrieves information of an Alias in Insprd. The `scope string` refers to the dApp that contains the Alias, represented with a dot separated query, such as **app1.app2**. The `key` parameter is the key of the Alias.  
So to search for an Alias `app2.chan1` that is inside of a dApp `app1` you would call:
```go
ac.Get(context.Background(), "app1", "app2.chan1")
```

### func \(\*AliasClient) Create

```go
func (ac *AliasClient) Create(ctx context.Context, scope, target string, alias *meta.Alias, dryRun bool) (diff.Changelog, error)
```
`Create` creates an Alias in Insprd. The `scope string` refers to the dApp that will contain the Alias, represented with a dot separated query, such as **app1.app2**. The Alias information such as its name and target will be extracted from the given Alias' definition.  
So to create an Alias that references `chan1` inside of a dApp `app1` you would call:
```go
ac.Create(context.Background(), "app1", "chan1", &meta.Alias{...})
```

### func \(\*AliasClient) Update

```go
func (ac *AliasClient) Update(ctx context.Context, scope, key string, alias *meta.Alias, dryRun bool) (diff.Changelog, error)
```
`Update` updates an Alias in Insprd. The `scope string` refers to the dApp that contains the Alias, represented with a dot separated query, such as **app1.app2**. The Alias information such as its name and target will be extracted from the given Alias' definition.  
So to update an Alias `app2.chan1` that is inside of a dApp `app1` you would call:
```go
ac.Create(context.Background(), "app1", "app2.chan1",  &meta.Alias{...})
```

### func \(\*AliasClient) Delete

```go
func (ac *AliasClient) Delete(ctx context.Context, scope, key string, dryRun bool) (diff.Changelog, error)
```
`Delete` deletes an Alias that exists in Insprd. The `scope string` refers to the dApp that contains the Alias, represented with a dot separated query, such as **app1.app2**. The `key` parameter is the key of the Alias to be deleted.   
So to delete an Alias `app2.chan1` that is inside of a dApp `app1` you would call:
```go
ac.Delete(context.Background(), "app1", "app2.chan1")
```

## Authorization

### func \(\*AuthClient) Init

```go
func (ac *AuthClient) Init(ctx context.Context, key string) (string, error)
```
`Init` is used to start the communication between Insprd and an UID Provider. The `key` parameter is expected to be Inspr's initialization key, which is defined in the Helm Chart. If the key informed is valid, `Init` returns a JWT token that is used by the UID Provider's admin user to communicate with Insprd.  
Supposing Insprd initialization key is "123456", to generate the admin JWT you'd do:
```go
ac.Init(context.Background(), "123456")
```

### func \(\*AuthClient) GenerateToken

```go
func (ac *AuthClient) GenerateToken(ctx context.Context, payload auth.Payload) (string, error)
```
`GenerateToken` receives a payload that contains some user's info, such as its identifier, permissions and refresh token, and generates a new JWT token for that user as long as the payload's data is valid.  
To do so, consider that the following payload structure is valid:
```go
validPayload := auth.Payload{
	UID: "user1",
	Permissions: map[string][]string{
        "": {
            CreateDapp,
            CreateChannel,
        },
    }
	Refresh: 1234567890,
	RefreshURL: "http://inspr-authsvc.com",
}

ac.GenerateToken(context.Background(), validPayload)
```

## Broker

### func \(\*BrokersClient) Get

```go
func (bc *BrokersClient) Get(ctx context.Context) (*models.BrokersDI, error)
```
`Get` returns a `models.BrokersDI` structure that contains a list of the currently installed brokers, as well as the default broker in Insprd:
```go
bc.Get(context.Background())
```