#!/bin/sh

docker buildx build --push --platform linux/arm64/v8,linux/amd64 -t mpgregor/fundalytics:0.0.3 .
docker buildx imagetools create mpgregor/fundalytics:0.0.3 --tag mpgregor/fundalytics:latest

## Or build individual images
# docker buildx build --load --platform linux/amd64 -t mpgregor/fundalytics:0.0.1_amd64 .
# docker buildx build --load --platform linux/arm64/v8 -t mpgregor/fundalytics:0.0.1_arm64 .
# docker push mpgregor/fundalytics:0.0.1_amd64
# docker push mpgregor/fundalytics:0.0.1_arm64
# docker manifest create mpgregor/fundalytics:0.0.1 \
#     --amend mpgregor/fundalytics:0.0.1_amd64 \
#     --amend mpgregor/fundalytics:0.0.1_arm64
# docker manifest push mpgregor/fundalytics:0.0.1

