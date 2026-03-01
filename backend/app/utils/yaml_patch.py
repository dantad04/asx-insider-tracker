"""
Monkey-patch for pyasx YAML compatibility.

PyASX uses deprecated yaml.load() without a Loader parameter.
This patch makes it work with modern PyYAML versions.
"""

import yaml

# Save original load function
_original_load = yaml.load

def patched_load(stream, Loader=yaml.FullLoader):
    """Patched yaml.load() that defaults to FullLoader if no Loader is specified."""
    return _original_load(stream, Loader=Loader)

# Apply the patch
yaml.load = patched_load
