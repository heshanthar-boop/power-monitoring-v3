# Global write guard for SCADA safety
_WRITE_UNLOCKED = False

def is_write_unlocked() -> bool:
    return bool(_WRITE_UNLOCKED)

def unlock_writes():
    global _WRITE_UNLOCKED
    _WRITE_UNLOCKED = True

def lock_writes():
    global _WRITE_UNLOCKED
    _WRITE_UNLOCKED = False
