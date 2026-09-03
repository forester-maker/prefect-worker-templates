# Image for runtime_storage.py on the ecs-spot / k8s-spot pools.
#
# The managed pool could not use this: `image` there is an enum of official Prefect
# images only. ECS and Kubernetes both expose a free-form `image`, which is the whole
# reason build/push exist in the sibling prefect.yaml.
FROM prefecthq/prefect:3.7.6-python3.11

# Dependencies first, so editing a flow does not invalidate the pip layer.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Flow code. runtime_storage.py currently lives in ../managed/flows/, so either move it
# to a shared flows/ directory at the repo root or point this COPY at its real location.
# Whatever path you COPY to must match `set_working_directory` in the pull step and the
# deployment `entrypoint`.
WORKDIR /opt/prefect
COPY flows/ /opt/prefect/flows/
