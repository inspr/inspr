## insprctl delete types

Delete types from scope

```
insprctl delete types [flags]
```

### Examples

```
  # Delete type from the default scope
 insprctl delete types <typename>

  # Delete type from a custom scope
 insprctl delete types <typename> --scope app1.app2

```

### Options

```
  -c, --config string   set the config file for the command
  -d, --dry-run         insprctl <command> --dry-run
  -h, --help            help for types
      --host string     set the host on the request header
  -s, --scope string    insprctl <command> --scope app1.app2
  -t, --token string    set the token for the command
```

### SEE ALSO

* [insprctl delete](insprctl_delete.md)	 - Delete component of object type

###### Auto generated by spf13/cobra on 17-Aug-2021
