## insprctl brokers

Retrieves brokers currently installed

### Synopsis

Broker takes a command of (brokers) to get installed brokers,
and a subcommand of (kafka) to install a new kafka broker

```
insprctl brokers [flags]
```

### Examples

```
  # get brokers
 insprctl brokers

```

### Options

```
  -h, --help   help for brokers
```

### SEE ALSO

* [insprctl](insprctl.md)	 - main command of the insprctl cli
* [insprctl brokers kafka](insprctl_brokers_kafka.md)	 - Configures a kafka broker on insprd by importing a valid yaml file carring configurations for one of the supported brokers

###### Auto generated by spf13/cobra on 17-Aug-2021