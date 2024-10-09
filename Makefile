ZIPDIR := mylangchain
CONNECTION := langchain
ZIPFILE := ${ZIPDIR}.zip
STAGE := packages

all: cleanall langchain zip put

help:           # Show this help.
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

langchain:		# Install LangChain locally
	pip install --no-binary=true --no-deps --target=${ZIPDIR} \
		. \
		langsmith==0.1.129 \
		langchain==0.3.1 \
		langchain-community==0.3.1 \
		langchain-core==0.3.7 \
		langchain-text-splitters==0.3.0

clean:		# Clean build, dist and egg-info for snowpoc
	test -d build && rm -fR build || true
	test -d dist && rm -fR dist || true
	test -d langchain_snowpoc.egg-info && rm -fR langchain_snowpoc.egg-info || true

cleanall: clean		# Run clean and remoce local LangChain
	test -d ${ZIPDIR} && rm -fR ${ZIPDIR} || true
	test -e ${ZIPFILE} && rm -f ${ZIPFILE} || true

zip:		# Create a zip with LangChain and snowpoc
	cd ${ZIPDIR} && zip -r ../${ZIPFILE} . && cd -

put:		# Copy zip to snowflake
	snow stage copy --overwrite ${ZIPFILE} @${STAGE}/ -c ${CONNECTION}
