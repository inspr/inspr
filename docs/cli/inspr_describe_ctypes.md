## inspr describe ctypes

Retrieves the full state of the channelType from a given namespace

```
inspr describe ctypes <ctype_name | ctype_path> [flags]
```

### Examples

```
  # Display the state of the given channelType on the default scope
 inspr describe ctypes hello_world

  # Display the state of the given channelType on a custom scope
 inspr describe ctypes --scope app1.app2 hello_world

  # Display the state of the given channelType by the path
 inspr describe ctypes app1.app2.hello_world

```

### Options

```
  -h, --help           help for ctypes
  -s, --scope string   inspr <command> --scope app1.app2
```

### SEE ALSO

* [inspr describe](inspr_describe.md)	 - Retrieves the full state of a component from a given namespace

###### Auto generated by spf13/cobra on 25-Mar-2021