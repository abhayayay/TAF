import signal


class GracefulKiller:
    kill_now = False

    def __init__(self):
        GracefulKiller.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        GracefulKiller.kill_now = True
