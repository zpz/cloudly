__all__ = ['get_host_ip']


import subprocess


def get_host_ip():
    """
    Get the IP address of the host machine from within a Docker container.
    """
    # INTERNAL_HOST_IP=$(ip route show default | awk '/default/ {print $3})')
    # another idea:
    # ip -4 route list match 0/0 | cut -d' ' -f3
    #
    # Usually the result is '172.17.0.1'
    # This is the "loopback" address used in code to connect to the host machine.
    # In most documentation this is said to be '127.0.0.1'.

    z = subprocess.check_output(['ip', '-4', 'route', 'list', 'match', '0/0'])
    z = z.decode()[len('default via ') :]
    return z[: z.find(' ')]
