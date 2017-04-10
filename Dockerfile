FROM gibiansky/ihaskell:latest

# Build:
# docker build -t ihaskell-karps .

# Install pandas for nice visualizations in Python
WORKDIR /tmp
RUN pip install pandas
RUN pip install requests

RUN rm /ihaskell/.stack-work/install/x86_64-linux/nightly-2015-08-15/7.10.2/bin/ihaskell

RUN mkdir /karps
WORKDIR /karps

COPY stack-ihaskell.yaml stack.yaml
COPY karps.cabal karps.cabal
COPY src src
COPY test test
COPY LICENSE LICENSE

RUN stack setup 7.10.2
RUN stack clean
RUN stack update
RUN stack install ipython-kernel-0.8.3.0
RUN stack install ihaskell-0.8.3.0
RUN stack install ihaskell-blaze-0.3.0.0
RUN stack install ihaskell-basic-0.3.0.0
RUN stack install


# Run the notebook
ENV PATH /karps/.stack-work/install/x86_64-linux/nightly-2015-08-15/7.10.2/bin:/root/.stack/snapshots/x86_64-linux/nightly-2015-08-15/7.10.2/bin:/root/.stack/programs/x86_64-linux/ghc-7.10.2/bin:/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN ihaskell install --stack
WORKDIR /karps
ENTRYPOINT stack exec -- jupyter notebook --NotebookApp.port=8888 '--NotebookApp.ip=*' --NotebookApp.notebook_dir=/karps
EXPOSE 8888
