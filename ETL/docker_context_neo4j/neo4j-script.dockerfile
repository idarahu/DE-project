FROM neo4j:5

COPY neo4j-start.sh /
ADD neo4j.conf /var/lib/neo4j/conf/neo4j.conf
COPY plugins /var/lib/neo4j/plugins

ENV EXTENSION_SCRIPT=/neo4j-start.sh
ENV NEO4J_AUTH=none
ENV NEO4L_PLUGINS='[\"apoc\", \"graph-data-science\"]'

EXPOSE 7474 7687