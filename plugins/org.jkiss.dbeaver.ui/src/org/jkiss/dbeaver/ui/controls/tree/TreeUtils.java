/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2021 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ui.controls.tree;

import org.eclipse.swt.widgets.TreeItem;
import org.jkiss.code.NotNull;
import org.jkiss.utils.ArrayUtils;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public final class TreeUtils {
    private TreeUtils() {
        // intentionally left blank
    }

    // javadoc
    public static void fixTreeCheckboxes(@NotNull TreeItem target) {
        TreeItemCheckboxState targetItemState = getState(target);
        if (targetItemState == TreeItemCheckboxState.BROKEN) { //TODO explain why
            setState(target, TreeItemCheckboxState.UNCHECKED, false);
            targetItemState = TreeItemCheckboxState.UNCHECKED;
        }

        // All items of the subtree with target as its root should have the same state. Let's do it.
        // Keep in mind that target is never HALFWAY_CHECKED, that's why we don't worry about it.
        Queue<TreeItem> queue = new ArrayDeque<>(getChildren(target));
        while (!queue.isEmpty()) {
            TreeItem item = queue.remove();
            setState(item, targetItemState, false);
            queue.addAll(getChildren(item));
        }

        // TODO: explain!
        // Now we need to fix target's parents. That's a complicated story.
        for (TreeItem item = target; item.getParentItem() != null; item = item.getParentItem()) {
            TreeItemCheckboxState itemState = getState(item);
            TreeItem parent = item.getParentItem();
            if (itemState == getState(parent)) {
                break;
            }
            if (itemState == TreeItemCheckboxState.HALFWAY_CHECKED || getChildren(parent).stream().anyMatch(i -> getState(i) != itemState)) {
                setState(parent, TreeItemCheckboxState.HALFWAY_CHECKED, false);
            } else {
                setState(parent, itemState, false);
            }
        }
    }

    @NotNull
    public static TreeItemCheckboxState getState(@NotNull TreeItem treeItem) {
        boolean checked = treeItem.getChecked();
        boolean grayed = treeItem.getGrayed();
        if (checked) {
            if (grayed) {
                return TreeItemCheckboxState.HALFWAY_CHECKED;
            }
            return TreeItemCheckboxState.FULLY_CHECKED;
        }
        if (grayed) {
            return TreeItemCheckboxState.BROKEN;
        }
        return TreeItemCheckboxState.UNCHECKED;
    }

    public static void setState(@NotNull TreeItem item, @NotNull TreeItemCheckboxState state) {
        setState(item, state, true);
    }

    private static void setState(@NotNull TreeItem item, @NotNull TreeItemCheckboxState state, boolean fixTree) {
        item.setChecked(state == TreeItemCheckboxState.FULLY_CHECKED || state == TreeItemCheckboxState.HALFWAY_CHECKED);
        item.setGrayed(state == TreeItemCheckboxState.HALFWAY_CHECKED || state == TreeItemCheckboxState.BROKEN);
        if (fixTree) {
            fixTreeCheckboxes(item);
        }
    }

    @NotNull
    private static List<TreeItem> getChildren(@NotNull TreeItem treeItem) {
        return ArrayUtils.safeArray(treeItem.getItems());
    }
}
