import { SidebarHeader } from './SidebarHeader';
import { SidebarList } from './SidebarList';

export function Sidebar() {
  return (
    <aside class="sb">
      <SidebarHeader />
      <div class="sbc" id="sbc">
        <SidebarList />
      </div>
    </aside>
  );
}
