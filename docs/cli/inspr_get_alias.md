## insprctl get alias

Get alias from context

```
insprctl get alias [flags]
```

### Examples

```
  # Get alias from the default scope
 insprctl get alias 

  # Get alias from a custom scope
 insprctl get alias --scope app1.app2

```

### Options

```
  -h, --help           help for alias
  -s, --scope string   inspr <command> --scope app1.app2
```

### SEE ALSO

* [insprctl get](inspr_get.md)	 - Retrieves the components from a given namespace

###### Auto generated by spf13/cobra on 22-Apr-2021