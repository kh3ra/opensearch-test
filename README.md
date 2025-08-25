```
# Create venv
python3 -m venv venv

# Activate venv
source venv/bin/activate

# Install requirements
pip install -r requirements.txt

# Start new session
tmux new -s opensearch

# Detach: Ctrl + b, d
# Attach: tmux attach -t opensearch-test
```
