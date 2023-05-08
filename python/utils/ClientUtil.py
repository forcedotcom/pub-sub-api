import getopt


def load_properties(filepath, sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value

    return props


def command_line_input(argument_list):
    argument_dict = {}
    options = "uplhotev"

    # Long options
    long_options = ["username=", "password=", "url=", "grpcHost=", "grpcPort=", "topic=", "tenantId=",
                    "apiVersion="]

    try:
        # Parsing argument
        arguments, values = getopt.getopt(argument_list, options, long_options)

        for currentArgument, currentValue in arguments:
            if currentArgument in ("-u", "--username"):
                argument_dict["username"] = currentValue

            elif currentArgument in ("-p", "--password"):
                argument_dict['password'] = currentValue

            elif currentArgument in ("-l", "--url"):
                argument_dict['url'] = currentValue

            elif currentArgument in ("-h", "--grpcHost"):
                argument_dict['grpcHost'] = currentValue

            elif currentArgument in ("-o", "--grpcPort"):
                argument_dict['grpcPort'] = currentValue

            elif currentArgument in ("-t", "--topic"):
                argument_dict['topic'] = currentValue

            elif currentArgument in ("-e", "--tenantId"):
                argument_dict['tenant_id'] = currentValue

            elif currentArgument in ("-v", "--apiVersion"):
                argument_dict['apiVersion'] = currentValue

        return argument_dict

    except getopt.error as err:
        # output error, and return with an error code
        print(str(err))
