One can get help on how to run the test utility by typing
python run_test.py --help
usage: run_test.py [-h] -cf CONFIG_FILE -tc TESTCASE -ti TIMEFRAME [-ta TAGS] [-rs RESET_SEQUENCE]

SFTP Testing

optional arguments:
  -h, --help            show this help message and exit
  -cf CONFIG_FILE, --config-file CONFIG_FILE
                        Config file path
  -tc TESTCASE, --testcase TESTCASE
                        Name of testcase
  -ti TIMEFRAME, --timeframe TIMEFRAME
                        How long the script to run
  -ta TAGS, --tags TAGS
                        Tags if any
  -rs RESET_SEQUENCE, --reset-sequence RESET_SEQUENCE
                        Reset Sequence

# TESTCASE value is read from the conf file.  You could have multiple test cases in the config but you can run one testcase at a time.
# TIMEFRAME allows you to run the test for a duration given in minutes
