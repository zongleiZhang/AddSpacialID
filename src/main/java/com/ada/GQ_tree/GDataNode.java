package com.ada.GQ_tree;

import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class GDataNode extends GNode implements Comparable<GDataNode>, Serializable {
    public int leafID;

    public GDataNode(){}

    public GDataNode(GDirNode parent, int position, GridRectangle centerRegion,
                     int elemNum, GTree tree, int leafID) {
        super(parent, position, centerRegion, elemNum, tree);
        this.leafID = leafID;
    }


    public void getLeafs(List<GDataNode> leafs) {
        leafs.add(this);
    }


    @Override
    GDataNode cloneNode() throws CloneNotSupportedException {
        return new GDataNode(null, -1, gridRegion.clone(), elemNum, null, leafID);
    }

    /**
     * 当根节点的4个子节点都是叶子结点且本叶节点是其中之一时，返回true，否则返回false
     */
    boolean isRootLeaf(){
        return parent.parent == null && parent.child[0] instanceof GDataNode
                && parent.child[1] instanceof GDataNode && parent.child[2] instanceof GDataNode
                && parent.child[3] instanceof GDataNode;
    }

    GNode adjustNode() {
        if (gridRegion.getArea() < 4)
            return this;
        int M = (int) (tree.globalLowBound*1.5);
        GDirNode dirNode;
        if ( elemNum >= M*15 ){
            dirNode = fourSplit(elemNum/4, (int)(0.9*elemNum/4), elemNum/4);
            for (int i = 0; i < 4; i++)
                dirNode.child[i] = ((GDataNode) dirNode.child[i]).adjustNode();
        }else if (elemNum < 2.2*tree.globalLowBound){
            if (isRoot()) {
                dirNode = fourSplit((int)(0.9*elemNum/4), elemNum / 4, elemNum / 4);
                return dirNode;
            }else {
                return this;
            }
        }else if (elemNum >= 2.2*tree.globalLowBound && elemNum < 5*M){
            if ( elemNum > 3*M )
                dirNode = fourSplit((int)(0.95*elemNum/4), (int)(0.90*elemNum/4), (int)(0.98*elemNum/4));
            else
                dirNode = fourSplit((int)(0.85*elemNum/4), (int)(0.9*elemNum/4), (int)(0.9*elemNum/4));
        }else if (elemNum >= 5*M && elemNum < 7*M){
            int m = tree.globalLowBound;
            dirNode = fourSplit( (int) (0.95*m), (int) (1.0*m), (int) (0.9*m));
            dirNode.child[3] = ((GDataNode) dirNode.child[3]).adjustNode();
        }else if (elemNum >= 7*M && elemNum < 8*M){
            dirNode = fourSplit( (int) (0.9*M),  (int) (0.95*M),  (int) (0.9*M));
            dirNode.child[3] = ((GDataNode) dirNode.child[3]).adjustNode();
        }else if (elemNum >= 8*M && elemNum < 8.5*M){
            dirNode = fourSplit( (int)(1.1*M), (int)(1.2*M), (int)(1.1*M));
            dirNode.child[3] = ((GDataNode) dirNode.child[3]).adjustNode();
        }else if (elemNum >= 8.5*M && elemNum < 11*M){
            int tmp = (int) (tree.globalLowBound*1.2);
            dirNode = fourSplit( tmp, tmp, (elemNum - tmp*2)/2);
            dirNode.child[2] = ((GDataNode) dirNode.child[2]).adjustNode();
            dirNode.child[3] = ((GDataNode) dirNode.child[3]).adjustNode();
        }else if (elemNum >= 11 * M){
            dirNode = fourSplit( (int) (0.9*M), (int)(0.9*((elemNum-M)/3)), (int)(0.9*((elemNum-M)/3)));
            for (int i = 1; i < 4; i++)
                dirNode.child[i] = ((GDataNode) dirNode.child[i]).adjustNode();
        }else {
            throw new IllegalArgumentException("Elem number error.");
        }
        return dirNode;
    }


    private GDirNode fourSplit(int num0, int num1, int num2){
        GDirNode node;
        node = new GDirNode(parent, position, gridRegion, elemNum, tree, new GNode[4]);
        double factor = (gridRegion.high.x-gridRegion.low.x+1.0)/(gridRegion.high.y-gridRegion.low.y+1.0);
        if (factor > 3.0){ //宽矩形
            int[] elemNums = tree.getElemNumArray(gridRegion, "x");
            int newX0 = getNewXY(elemNums, 0, num0);
            if (newX0 > elemNums.length-4){
                newX0 = elemNums.length-4;
            }
            int newX1 = getNewXY(elemNums, newX0+1, num1);
            if (newX1 > elemNums.length-3){
                newX1 = elemNums.length-3;
            }
            int newX2 = getNewXY(elemNums, newX1+1, num2);
            if (newX2 > elemNums.length-2){
                newX2 = elemNums.length-2;
            }
            GridRectangle gr;
            gr = new GridRectangle(gridRegion.low, new GridPoint(gridRegion.low.x + newX0, gridRegion.high.y));
            node.child[0] = new GDataNode(node, 0, gr, getElemNum(elemNums, 0, newX0), tree, -1);
            gr = new GridRectangle(new GridPoint(gridRegion.low.x+newX0+1, gridRegion.low.y), new GridPoint(gridRegion.low.x+newX1, gridRegion.high.y));
            node.child[1] = new GDataNode(node, 1, gr, getElemNum(elemNums, newX0+1, newX1), tree, -1);
            gr = new GridRectangle(new GridPoint(gridRegion.low.x+newX1+1, gridRegion.low.y), new GridPoint(gridRegion.low.x+newX2, gridRegion.high.y));
            node.child[2] = new GDataNode(node, 2, gr, getElemNum(elemNums, newX1+1, newX2), tree, -1);
            gr = new GridRectangle(new GridPoint(gridRegion.low.x+newX2+1, gridRegion.low.y), gridRegion.high);
            node.child[3] = new GDataNode(node, 3, gr, getElemNum(elemNums, newX2+1, elemNums.length-1), tree, -1);
        }else if (factor < 0.33){ //长矩形
            int[] elemNums = tree.getElemNumArray(gridRegion, "y");
            int newY0 = getNewXY(elemNums, 0, num0);
            if (newY0 > elemNums.length-4){
                newY0 = elemNums.length-4;
            }
            int newY1 = getNewXY(elemNums, newY0+1, num1);
            if (newY1 > elemNums.length-3){
                newY1 = elemNums.length-3;
            }
            int newY2 = getNewXY(elemNums, newY1+1, num2);
            if (newY2 > elemNums.length-2){
                newY2 = elemNums.length-2;
            }
            GridRectangle gr;
            gr = new GridRectangle(gridRegion.low, new GridPoint(gridRegion.high.x, gridRegion.low.y + newY0));
            node.child[0] = new GDataNode(node, 0, gr, getElemNum(elemNums, 0, newY0), tree, -1);
            gr = new GridRectangle(new GridPoint(gridRegion.low.x, gridRegion.low.y+newY0+1), new GridPoint(gridRegion.high.x, gridRegion.low.y+newY1));
            node.child[1] = new GDataNode(node, 1, gr, getElemNum(elemNums, newY0+1, newY1), tree, -1);
            gr = new GridRectangle(new GridPoint(gridRegion.low.x, gridRegion.low.y+newY1+1), new GridPoint(gridRegion.high.x, gridRegion.low.y+newY2));
            node.child[2] = new GDataNode(node, 2, gr, getElemNum(elemNums, newY1+1, newY2), tree, -1);
            gr = new GridRectangle(new GridPoint(gridRegion.low.x, gridRegion.low.y+newY2+1), gridRegion.high);
            node.child[3] = new GDataNode(node, 3, gr, getElemNum(elemNums, newY2+1, elemNums.length-1), tree, -1);
        }else { //正方形
            int[] elemNums = tree.getElemNumArray(gridRegion, "x");
            int newX = getNewXY(elemNums,0, num0 + num1);
            if (newX == elemNums.length-1) newX--;

            GridRectangle rectangle00 = new GridRectangle(gridRegion.low, new GridPoint(gridRegion.low.x + newX, gridRegion.high.y));
            int[] elemNums00 = tree.getElemNumArray(rectangle00, "y");
            int newY0 = getNewXY(elemNums00, 0, num0);
            if (newY0 == elemNums00.length-1) newY0--;
            int elemNum0 = getElemNum(elemNums00, 0, newY0);
            int elemNum1 = getElemNum(elemNums00, newY0+1, elemNums00.length-1);
            GridRectangle rectangle0 = new GridRectangle(gridRegion.low, new GridPoint(gridRegion.low.x + newX, gridRegion.low.y + newY0));
            GridRectangle rectangle1 = new GridRectangle(new GridPoint(gridRegion.low.x, gridRegion.low.y + newY0 + 1), new GridPoint(gridRegion.low.x + newX, gridRegion.high.y));

            GridRectangle rectangle11 = new GridRectangle(new GridPoint(gridRegion.low.x + newX+1, gridRegion.low.y), gridRegion.high);
            int[] elemNums11 = tree.getElemNumArray(rectangle11, "y");
            int newY1 = getNewXY(elemNums11, 0, num2);
            if (newY1 == elemNums11.length-1) newY1--;
            int elemNum2 = getElemNum(elemNums11, 0, newY1);
            int elemNum3 = getElemNum(elemNums11, newY1+1, elemNums11.length-1);
            GridRectangle rectangle2 = new GridRectangle(new GridPoint(gridRegion.low.x + newX + 1, gridRegion.low.y), new GridPoint(gridRegion.high.x, gridRegion.low.y +newY1));
            GridRectangle rectangle3 = new GridRectangle(new GridPoint(gridRegion.low.x + newX + 1, gridRegion.low.y + newY1 + 1), gridRegion.high);

            node.child[0] = new GDataNode(node, 0, rectangle0, elemNum0, tree, -1);
            node.child[1] = new GDataNode(node, 1, rectangle1, elemNum1, tree, -1);
            node.child[2] = new GDataNode(node, 2, rectangle2, elemNum2, tree, -1);
            node.child[3] = new GDataNode(node, 3, rectangle3, elemNum3, tree, -1);
            if (parent != null){
                parent.child[position] = node;
            }
        }

        return node;
    }

    int updateElemNum(){
        elemNum = tree.getRangeEleNum(this.gridRegion);
        return elemNum;
    }

    /**
     * 获取数组elemNums，在start到
     */
    private int getElemNum(int[] elemNums, int start, int end) {
        int res = 0;
        for (int j = start; j <= end; j++)
            res += elemNums[j];
        return res;
    }

    /**
     * 在整数数组eleNums中找出前n位数，使它们和是大于bound的最小数。
     */
    private int getNewXY(int[] elemNums, int from, int bound) {
        int tmp = elemNums[from];
        int newX = from;
        while (tmp < bound){
            newX++;
            tmp += elemNums[newX];
        }
        return newX;
    }

    public GDataNode searchGPoint(GridPoint gPoint) {
        return this;
    }

    public void getIntersectLeafNodes(Point point, List<GDataNode> leafs) {
        leafs.add(this);
    }

    @Override
    public int compareTo(@NotNull GDataNode o) {
        return Integer.compare(elemNum,o.elemNum);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GDataNode dataNode = (GDataNode) o;
        if (leafID != dataNode.leafID)
            return false;
        return true;
    }

    @Override
    void setNodeTree(GTree tree) {
        this.tree = tree;
    }
}






















