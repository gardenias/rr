"use client";

import * as DialogPrimitive from "@radix-ui/react-dialog";
import { X } from "lucide-react";
import type { ReactElement, ReactNode } from "react";
import { cn } from "@/lib/utils";

interface DrawerProps {
  trigger: ReactElement;
  title: string;
  description: string;
  children: ReactNode;
  width?: 480 | 560;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}

export function Drawer({
  trigger,
  title,
  description,
  children,
  width = 560,
  open,
  onOpenChange,
}: DrawerProps) {
  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Trigger asChild>{trigger}</DialogPrimitive.Trigger>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="dialog-overlay" />
        <DialogPrimitive.Content
          className={cn("drawer-content", width === 480 ? "drawer-480" : "drawer-560")}
        >
          <div className="dialog-heading">
            <div>
              <DialogPrimitive.Title>{title}</DialogPrimitive.Title>
              <DialogPrimitive.Description>{description}</DialogPrimitive.Description>
            </div>
            <DialogPrimitive.Close className="icon-button" aria-label="Close drawer">
              <X aria-hidden="true" size={16} />
            </DialogPrimitive.Close>
          </div>
          <div className="drawer-body">{children}</div>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}
