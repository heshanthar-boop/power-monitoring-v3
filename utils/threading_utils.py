import threading

class StoppableThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_event = threading.Event()
        # Backward-compat: some older code checks self._stop
        # (do not remove without updating all call sites)
        self._stop = self.stop_event
        self.daemon = True

    def stop(self):
        self.stop_event.set()
