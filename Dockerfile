FROM dock.mau.dev/maubot/maubot

VOLUME /src
CMD mbc build /src && cp /opt/maubot/*.mbp /src