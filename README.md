# Configuration

All the parameters of the Storyline system are listed in `config.yml`. Please use it as a boilerplate and copy it to a local directory and config it specifically for some dataset.

# Execution

`pipeline.py` is the main function to be executed. Please use your `config.yml` as a commandline parameter. For example:

`python pipeline.py /home/shiguang/Projects/evtrack/data/Protests/config.yml`

# Integration to Apollo

`pipeline.py` provides a method `process()` that takes a timestamp `ts` and the config `cfg` as parameters. Make sure the configuration file is initialized with your specific one, and call `process()` when each "60m.cluster_head.txt" file is created.
