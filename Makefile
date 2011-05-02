
VERSION := $(shell cat VERSION)

DOCS_SRC := docs
DOCS := $(DOCS_SRC)/_build/html
BuildDir := build
JAR := $(BuildDir)/seal.jar
Tarball := $(BuildDir)/seal.tar.gz

.PHONY: clean distclean

all: dist
	
dist: $(Tarball)

$(Tarball): jbuild pbuild
	rm -rf $(BuildDir)/seal
	mkdir $(BuildDir)/seal $(BuildDir)/seal/bin
	ln $(JAR) $(BuildDir)/seal/seal.jar
	ln bin/* $(BuildDir)/seal/bin
	cp -r $(shell python -c "import sys; print 'build/lib/python%d.%d/site-packages/bl' % sys.version_info[0:2]") $(BuildDir)/seal/bl
	tar -C $(BuildDir) -czf $(Tarball) seal

jbuild: $(JAR)

$(JAR): build.xml src
	ant

pbuild: bl
	python setup.py install --prefix $(BuildDir)

doc: $(DOCS)

$(DOCS): $(DOCS_SRC)
	make -C $< html

upload-docs: doc
	rsync -avz --delete -e ssh --exclude=.buildinfo docs/_build/html/ ilveroluca,biodoop-seal@web.sourceforge.net:/home/project-web/biodoop-seal/htdocs


clean:
	ant clean
	rm -rf build
	make -C docs clean
	rmdir docs/_build docs/_templates docs/_static || true
	find bl -name '*.pyc' -print0 | xargs -0  rm -f
	find bl/lib/seq/aligner/bwa/libbwa/ -name '*.ol' -o -name '*.o' -print0 | xargs -0  rm -f
	rm -f bl/lib/seq/aligner/bwa/libbwa/bwa
	find . -name '*~' -print0 | xargs -0  rm -f

distclean: clean
