## insprctl delete channels

Delete channels from scope

```
insprctl delete channels [flags]
```

### Examples

```
  # Delete channel from the default scope
 insprctl delete channels <channelname>

  # Delete channels from a custom scope
 insprctl delete channels <channelname> --scope app1.app2

```

### Options

```
  -c, --config string   set the config file for the command
  -h, --help            help for channels
  -s, --scope string    insprctl <command> --scope app1.app2
  -t, --token string    set the token for the command
```

### SEE ALSO

* [insprctl delete](insprctl_delete.md)	 - Delete component of object type

###### Auto generated by spf13/cobra on 15-Jun-2021