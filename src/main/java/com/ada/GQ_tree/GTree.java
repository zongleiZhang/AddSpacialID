package com.ada.GQ_tree;

import com.ada.Hungarian.Hungary;
import com.ada.common.Collections;
import com.ada.common.Constants;
import com.ada.common.Path;
import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.geometry.Point;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.*;

public class GTree implements Serializable {

    /**
     * 根节点
     */
    private GDirNode root;

    /**
     * 全局索引叶节点索引项数量的下届
     */
    public int globalLowBound;

    /**
     * 密度网格
     */
    public int[][] density;

    public int leafNum;

    /**
     * 分配叶节点ID的功能成员
     */
    DispatchLeafID dispatchLeafID;

    public GTree() {
        dispatchLeafID = new DispatchLeafID();
        int gridDensity = Constants.gridDensity;
        root = new GDirNode(null,-1,
                new GridRectangle(new GridPoint(0,0), new GridPoint(gridDensity, gridDensity)),
                0,this, new GNode[4]);
        GridRectangle gridRectangle;
        gridRectangle = new GridRectangle(new GridPoint(0,0), new GridPoint(gridDensity/2, gridDensity/2));
        root.child[0] = new GDataNode(root,0, gridRectangle,0, this,dispatchLeafID.getLeafID());
        gridRectangle =  new GridRectangle(new GridPoint(0,(gridDensity/2)+1), new GridPoint(gridDensity/2, gridDensity));
        root.child[1] = new GDataNode(root,1, gridRectangle,0, this,dispatchLeafID.getLeafID());
        gridRectangle = new GridRectangle(new GridPoint((gridDensity/2)+1,0), new GridPoint(gridDensity, (gridDensity/2)));
        root.child[2] = new GDataNode(root,2, gridRectangle,0, this,dispatchLeafID.getLeafID());
        gridRectangle = new GridRectangle(new GridPoint((gridDensity/2)+1,(gridDensity/2)+1), new GridPoint(gridDensity, gridDensity));
        root.child[3] = new GDataNode(root,3, gridRectangle,0,  this,dispatchLeafID.getLeafID());
        leafNum = 4;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GTree gTree = (GTree) o;
        if (globalLowBound != gTree.globalLowBound ||
                leafNum != gTree.leafNum)
            return false;
        if (!Objects.equals(root, gTree.root))
            return false;
        if (!Objects.equals(dispatchLeafID, gTree.dispatchLeafID))
            return false;
        return true;
    }

    public void cloneRoot(GTree tree) {
        try {
            this.root = tree.root.cloneNode();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
    }

    static class DispatchLeafID implements Serializable{
        List<Integer> usedLeafID;

        List<Integer> canUseLeafID;

        DispatchLeafID(){
            usedLeafID = new ArrayList<>();
            canUseLeafID = new ArrayList<>();
            for (int i = Constants.dividePartition-1; i >= 0; i--)
                canUseLeafID.add(i);
        }

        Integer getLeafID(){
            if (canUseLeafID.isEmpty()){
                throw new IllegalArgumentException("LeafID is FPed");
            }else {
                Integer leafID = canUseLeafID.remove(canUseLeafID.size() - 1);
                usedLeafID.add(leafID);
                return leafID;
            }
        }

        void discardLeafID(Integer leafID){
            canUseLeafID.add(leafID);
            usedLeafID.remove(leafID);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DispatchLeafID that = (DispatchLeafID) o;
            if (!Collections.collectionsEqual(usedLeafID, that.usedLeafID))
                return false;
            if (!Collections.collectionsEqual(canUseLeafID, that.canUseLeafID))
                return false;
            return true;
        }
    }

    public boolean check(){
        List<GDataNode> leafs = new ArrayList<>();
        root.getLeafs(leafs);
        return root.check();
    }


    /**
     * 获取一个矩形内的元素数量
     */
    int getRangeEleNum(GridRectangle range) {
        int res = 0;
        for (int i = range.low.x; i <= range.high.x; i++) {
            for (int j = range.low.y; j <= range.high.y; j++) {
                res += density[i][j];
            }
        }
        return res;
    }

    /**
     * 获取矩形范围region内某个坐标方向上的索引项密度
     * @param region 矩形范围
     * @param axis 坐标轴方向
     * @return 数据密度
     */
    int[] getElemNumArray(GridRectangle region, String axis) {
        int[] res;
        int tmp;
        if ("x".equals(axis)){
            res = new int[region.high.x - region.low.x + 1];
            for (int i = region.low.x; i <= region.high.x; i++) {
                tmp = 0;
                for (int j = region.low.y; j <= region.high.y; j++)
                    tmp += density[i][j];
                res[i - region.low.x] = tmp;
            }
        }else if ("y".equals(axis)){
            res = new int[region.high.y - region.low.y + 1];
            for (int i = region.low.y; i <= region.high.y; i++) {
                tmp = 0;
                for (int j = region.low.x; j <= region.high.x; j++)
                    tmp += density[j][i];
                res[i - region.low.y] = tmp;
            }
        }else {
            throw new IllegalArgumentException("axis is error.");
        }
        return res;
    }

    /**
     * 根据新的网格密度数据更新树结构
     */
    public List<Tuple2<GNode, GNode>> updateTree(int[][] density){
        //根据新的密度网格density更新全局索引的每个节点上的elemNum信息
        this.density = density;
        root.updateElemNum();

        /*
         * 根据全局索引项的数量，计算叶节点存储索引项的下届globalLowBound，
         * 公式为：globalLowBound = 索引项总量/局部索引任务的并行度
         * 考虑到局部索引的数量不能超过局部索引任务的并行度，且要非常接近这个并行度，
         * 这里用一个逐渐增加的权重因子factor来逐渐提升globalLowBound的值
         */
        int itemNum = getRangeEleNum(new GridRectangle(new GridPoint(0, 0), new GridPoint(Constants.gridDensity, Constants.gridDensity)));
        int glb = itemNum/ Constants.dividePartition;
        double factor = 0.6;
        List<Tuple3<GNode, GNode, Integer>> tup3s = new ArrayList<>();
        int leafNum;

        do {
            globalLowBound = (int) (factor * glb);
            factor += 0.1;
            tup3s.clear();
            leafNum = this.leafNum;
            List<GNode> list = new ArrayList<>();
            getAdjustNode(list);
            for (GNode oldNode : list) {
                GDataNode dataNode = new GDataNode(null, -1, oldNode.gridRegion,
                        oldNode.elemNum, oldNode.tree, -1);
                GNode newNode = dataNode.adjustNode();
                List<GDataNode> oldLeaves = new ArrayList<>();
                List<GDataNode> newLeaves = new ArrayList<>();
                newNode.getLeafs(newLeaves);
                oldNode.getLeafs(oldLeaves);
                leafNum += newLeaves.size() - oldLeaves.size();
                tup3s.add(new Tuple3<>(oldNode, newNode, oldLeaves.size() - newLeaves.size()));
            }
        }while (leafNum > Constants.dividePartition);
        this.leafNum = leafNum;
        tup3s.sort((o1, o2) -> o2.f2.compareTo(o1.f2));
        tup3s.forEach(tup -> dispatchLeafID(tup.f0, tup.f1));
        this.density = null;
        return (List<Tuple2<GNode, GNode>>) Collections.changeCollectionElem(tup3s, t3 -> new Tuple2<>(t3.f0, t3.f1));
    }

    public void replaceNode(List<Tuple2<GNode, GNode>> tup2s) {
        tup2s.forEach(tup -> {
            GNode oldNode = tup.f0;
            GNode newNode = tup.f1;
            if (oldNode.isRoot()) {
                root = (GDirNode) newNode;
            }else {
                newNode.parent = oldNode.parent;
                newNode.position = oldNode.position;
                oldNode.parent.child[oldNode.position] = newNode;
            }
        });
    }

    /**
     * 获取需要调整结构的子树集合(GQ)
     * @param nodes 记录需要调整结构的子树
     */
    private void getAdjustNode(List<GNode> nodes) {
        for (GDataNode leafNode : getAllLeafs()) {
            if ( leafNode.elemNum > 2*globalLowBound ||
                    (leafNode.elemNum < globalLowBound && !leafNode.isRootLeaf()) ) {
                GNode addNode;
                if (leafNode.elemNum < 4.5 * globalLowBound){ //叶节点不可以分裂
                    addNode = leafNode.parent;
                    while (true) {
                        if (addNode.isRoot()){
                            break;
                        }else if (addNode.elemNum < globalLowBound) {
                            addNode = addNode.parent;
                        }else if (addNode.elemNum <= 2* globalLowBound ){
                            break;
                        }else if ( addNode.elemNum < 4.5 * globalLowBound ) {
                            addNode = addNode.parent;
                        }else {
                            break;
                        }
                    }
                }else { //叶节点可以分裂
                    addNode = leafNode;
                }
                boolean hasAncestor = false;
                Path path0 = new Path(addNode);
                for (Iterator<GNode> ite = nodes.iterator(); ite.hasNext(); ){
                    GNode node = ite.next();
                    Path path1 = new Path(node);
                    int tmp = Path.isSameWay(path0, path1);
                    if (tmp == 1) ite.remove();
                    if (tmp == -1){
                        hasAncestor = true;
                        break;
                    }
                }
                if (!hasAncestor) nodes.add(addNode);
            }
        }
    }

    /**
     * 子树调整前后的节点是oldNode，newNode。为newNode中的叶节点重新分配ID
     */
    private void dispatchLeafID(GNode oldNode, GNode newNode){
        if (oldNode instanceof GDirNode && newNode instanceof GDirNode){ //多分多
            GDirNode newDirNode = (GDirNode) newNode;
            GDirNode oldDirNode = (GDirNode) oldNode;
            List<GDataNode> newLeafNodes = new ArrayList<>();
            newDirNode.getLeafs(newLeafNodes);
            List<GDataNode> oldLeafNodes = new ArrayList<>();
            oldDirNode.getLeafs(oldLeafNodes);
            int[][] matrix = new int[newLeafNodes.size()][oldLeafNodes.size()];
            for (int i = 0; i < matrix.length; i++) {
                GDataNode dataNode = newLeafNodes.get(i);
                GridPoint gPoint = new GridPoint();
                for (int j = dataNode.gridRegion.low.x; j <= dataNode.gridRegion.high.x; j++) {
                    for (int k = dataNode.gridRegion.low.y; k <= dataNode.gridRegion.high.y; k++) {
                        gPoint.x = j;
                        gPoint.y = k;
                        GDataNode gDataNode = oldDirNode.searchGPoint(gPoint);
                        int index = oldLeafNodes.indexOf(gDataNode);
                        matrix[i][index] += density[j][k];
                    }
                }
            }
            int[][] leafIDMap = redisPatchLeafID(matrix, 100*globalLowBound);
            for (int[] map : leafIDMap) {
                int leafId = oldLeafNodes.get(map[1]).leafID;
                newLeafNodes.get(map[0]).setLeafID(leafId);
            }
            if (newLeafNodes.size() > oldLeafNodes.size()){ //少分多
                Set<Integer> reassigningID = new HashSet<>();
                for (int[] map : leafIDMap)
                    reassigningID.add(map[0]);
                for (int i = 0; i < newLeafNodes.size(); i++) {
                    if (!reassigningID.contains(i)){
                        Integer leafID = dispatchLeafID.getLeafID();
                        newLeafNodes.get(i).setLeafID(leafID);
                    }
                }
            }
            if (newLeafNodes.size() < oldLeafNodes.size()){ //多分少
                Set<Integer> reassignedID = new HashSet<>();
                for (int[] map : leafIDMap)
                    reassignedID.add(map[1]);
                for (int i = 0; i < oldLeafNodes.size(); i++) {
                    if (!reassignedID.contains(i)){
                        Integer leafID = oldLeafNodes.get(i).leafID;
                        dispatchLeafID.discardLeafID(leafID);
                    }
                }
            }
        }else if(oldNode instanceof GDataNode && newNode instanceof GDirNode){ //一分多
            GDirNode newDirNode = (GDirNode) newNode;
            List<GDataNode> newLeafNodes = new ArrayList<>();
            newDirNode.getLeafs(newLeafNodes);
            int maxNumLeaf = getMaxElemNumIndex(newLeafNodes);
            Integer leafID = ((GDataNode)oldNode).leafID;
            newLeafNodes.get(maxNumLeaf).setLeafID(leafID);
            newLeafNodes.remove(maxNumLeaf);
            for (GDataNode newLeafNode : newLeafNodes) {
                leafID = dispatchLeafID.getLeafID();
                newLeafNode.setLeafID(leafID);
            }
        }else if(oldNode instanceof GDirNode && newNode instanceof GDataNode){ //多合一
            GDirNode oldDirNode = (GDirNode) oldNode;
            List<GDataNode> oldLeafNodes = new ArrayList<>();
            oldDirNode.getLeafs(oldLeafNodes);
            int maxNumLeaf = getMaxElemNumIndex(oldLeafNodes);
            int leafID = oldLeafNodes.get(maxNumLeaf).leafID;
            ((GDataNode)newNode).setLeafID(leafID);
            oldLeafNodes.remove(maxNumLeaf);
            for (GDataNode oldLeafNode : oldLeafNodes) dispatchLeafID.discardLeafID(oldLeafNode.leafID);
        }else {
            throw new IllegalArgumentException("GNode type error.");
        }
    }


    /**
     * 使用Hungarian Algorithm重新分配leafID.
     * @param matrix 分裂前后元素映射数量关系
     * @return 分配结果
     */
    public static int[][] redisPatchLeafID(int[][] matrix, int upBound){
        int rowNum = matrix.length;
        int colNum = matrix[0].length;
        int[][] newMatrix;
        if (rowNum > colNum){
            newMatrix = new int[rowNum][rowNum];
            for (int i = 0; i < newMatrix.length; i++) {
                for (int j = 0; j < newMatrix[0].length; j++) {
                    if (j >= matrix[0].length)
                        newMatrix[i][j] = upBound;
                    else
                        newMatrix[i][j] = upBound - matrix[i][j];
                }
            }
        }else {
            newMatrix = new int[colNum][];
            for (int i = 0; i < newMatrix.length; i++) {
                if (i < matrix.length) {
                    newMatrix[i] = new int[colNum];
                    for (int j = 0; j < colNum; j++)
                        newMatrix[i][j] = upBound - matrix[i][j];
                }else {
                    newMatrix[i] = new int[colNum];
                    Arrays.fill(newMatrix[i],upBound);
                }
            }
        }
        int[][] res = new Hungary().calculate(newMatrix);
        List<Tuple2<Integer,Integer>> list = new ArrayList<>();
        for (int[] re : res)
            list.add(new Tuple2<>(re[0],re[1]));
        if (rowNum > colNum)
            list.removeIf(ints -> ints.f1 >= colNum);
        else
            list.removeIf(ints -> ints.f0 >= rowNum);
        res = new int[list.size()][2];
        for (int i = 0; i < res.length; i++) {
            res[i][0] = list.get(i).f0;
            res[i][1] = list.get(i).f1;
        }
        return res;
    }


    /**
     * 在叶节点集合leafNodes中找出elemNum最大的节点
     */
    private int getMaxElemNumIndex(List<GDataNode> leafNodes) {
        int maxNum = -1;
        int maxIndex = -1;
        for (int i = 0; i < leafNodes.size(); i++) {
            int currentNum = leafNodes.get(i).elemNum;
            if (currentNum > maxNum){
                maxIndex = i;
                maxNum = currentNum;
            }
        }
        return maxIndex;
    }

    /**
     * 返回树中与point相交的叶节点的ID集合
     */
    public List<Integer> searchLeafNodes(Point point){
        List<GDataNode> list = getIntersectLeafNodes(point);
        return (List<Integer>) Collections.changeCollectionElem(list, node -> node.leafID);
    }

    /**
     * 返回树中与point相交的叶节点集合
     */
    public List<GDataNode> getIntersectLeafNodes(Point point){
        List<GDataNode> list = new ArrayList<>(1);
        root.getIntersectLeafNodes(point, list);
        return list;
    }

    public List<GDataNode> getAllLeafs(){
        List<GDataNode> list = new ArrayList<>();
        root.getLeafs(list);
        return list;
    }
}










