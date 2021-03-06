## insprctl cluster init

Init configures insprd's default token

```
insprctl cluster init [flags]
```

### Examples

```
  # init insprd as admin
 insprctl cluster init <admin_password>

```

### Options

```
  -c, --config string   set the config file for the command
  -h, --help            help for init
      --host string     set the host on the request header
  -s, --scope string    insprctl <command> --scope app1.app2
  -t, --token string    set the token for the command
```

### SEE ALSO

* [insprctl cluster](insprctl_cluster.md)	 - Configure aspects of your insprd cluster

###### Auto generated by spf13/cobra on 17-Aug-2021
