package com.ada.GQ_tree;

import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
public class GDirNode extends GNode implements Serializable {

    public GNode[] child;

    public GDirNode(){}

    public GDirNode(GDirNode parent, int position, GridRectangle centerRegion, int elemNum, GTree tree, GNode[] child) {
        super(parent, position, centerRegion, elemNum, tree);
        this.child = child;
    }

    public void getLeafs(List<GDataNode> leafs) {
        for (GNode gNode : child)
            gNode.getLeafs(leafs);
    }

    @Override
    GDirNode cloneNode() throws CloneNotSupportedException {
        GDirNode node = new GDirNode(null, -1, gridRegion.clone(), elemNum, null, new GNode[4]);
        for (int i = 0; i < 4; i++) {
            node.child[i] = child[i].cloneNode();
        }
        return node;
    }

    boolean checkGDirNode() {
        if (elemNum != child[0].elemNum + child[1].elemNum + child[2].elemNum + child[3].elemNum)
            throw new IllegalArgumentException("elemNum error");
        if ( gridRegion.low.y != child[0].gridRegion.low.y ||
                child[0].gridRegion.high.y+1 != child[1].gridRegion.low.y ||
                child[1].gridRegion.high.y != gridRegion.high.y ||
                gridRegion.low.y != child[2].gridRegion.low.y ||
                child[2].gridRegion.high.y+1 != child[3].gridRegion.low.y ||
                child[3].gridRegion.high.y != gridRegion.high.y ||
                gridRegion.low.x != child[0].gridRegion.low.x ||
                gridRegion.low.x != child[1].gridRegion.low.x ||
                gridRegion.high.x != child[2].gridRegion.high.x ||
                gridRegion.high.x != child[3].gridRegion.high.x ||
                child[0].gridRegion.high.x != child[1].gridRegion.high.x ||
                child[0].gridRegion.high.x+1 != child[2].gridRegion.low.x ||
                child[0].gridRegion.high.x+1 != child[3].gridRegion.low.x)
            throw new IllegalArgumentException("region error");
        return true;
    }

    public GDataNode searchGPoint(GridPoint gPoint) {
        for (GNode gNode : child) {
            if (gNode.gridRegion.isInternal(gPoint))
                return gNode.searchGPoint(gPoint);
        }
        return null;
    }

    public void getIntersectLeafNodes(Point point, List<GDataNode> leafs) {
        for (GNode node : child) {
            if (node.region.isInternal(point)) {
                node.getIntersectLeafNodes(point, leafs);
            }
        }
    }


    int updateElemNum(){
        elemNum = 0;
        for (GNode gNode : child) {
            elemNum += gNode.updateElemNum();
        }
        return elemNum;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GDirNode dirNode = (GDirNode) o;
        if (child==dirNode.child)
            return true;
        if (child==null || dirNode.child==null)
            return false;
        int length = child.length;
        if (dirNode.child.length != length)
            return false;
        for (int i=0; i<length; i++) {
            GNode o1 = child[i];
            GNode o2 = dirNode.child[i];
            if (!(Objects.equals(o1, o2)))
                return false;
        }
        return true;
    }

    @Override
    void setNodeTree(GTree tree) {
        this.tree = tree;
        for (GNode gNode : child) {
            gNode.setNodeTree(tree);
        }
    }

}
