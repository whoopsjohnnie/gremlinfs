
# GremlinFS
GremlinFS

FS strategies

Things to answer
- How do we represent a folder
- How do we represent a folder structure
- How do we represent a file
- How do we represent hard and soft links
- Would like to support graphs that were not created via gremlinFS,
	i.e. try to navigate and use general graphs


1) File as V; Folder and Links as E
	- Problematic when mounting to a V, FS expects folder, but mounts as file
	- Needs to mount to E?

2) Folder as V; File, Folder structure and Links as E
	- Can freely cd around all Vs, reference file by E in V
	- How do we distinguish between File as E and cd as E?
	- How do we read file data?
	- Can limit cd to only follow E with name "in", or "group", ...
	- Works well with mounting any V as root
	- If we limit cd to only follow "in", then we lose the free folder structure
	- Lose ability to have generic V classes with schemas represent file types
	- Must read data from a V which represent file data?

3) Special V class for Folders
	- Must mount to a V of this type?
	- Anything not of that V class is a File?
	- E represents links and folder structure

4) Special V class for Files
	- Lose ability to have generic classes with schemas represent file types
	- Suboptimal


Proposal:
	- Folder as V class?
	- File as generic V
	- Folder membership as link to folder V class?
	- Folder membershop as link with name "in"?
	- Mount root, must be of folder V class?


	- Folders are generic V class
	- Files are properties in V class
		- Block: Cannot move file from one folder to another


	- V('/'), /
	- V('readme.txt'), ./readme.txt
	- V('/') <- E('in') <- V('readme.txt'), /readme.txt, cat /readme.txt
	- V('/') <- E('readme.txt') <- V(...), /readme.txt, cat /readme.txt
	- But how do we distinguish that V('/') is a folder and V('readme.txt') is a file?
	cd('/in/readme.txt')
	[], [in], [readme.txt]




