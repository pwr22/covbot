FROM dock.mau.dev/maubot/maubot

VOLUME /src
CMD mbc build /src && cp /opt/maubot/dev.shortestpath.covbot-v0.0.1.mbp /src