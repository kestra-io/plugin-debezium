FROM mongo:latest
RUN echo "password" > /mongo-keyfile \
  && chmod 600 /mongo-keyfile \
  && chown 999 /mongo-keyfile \
  && chgrp 999 /mongo-keyfile
CMD ["--bind_ip_all", "--keyFile", "/mongo-keyfile", "--replSet", "rs0"]